from kfp.dsl import Input, Output, Dataset, Model, Metrics, component

@component(base_image="python:3.11", packages_to_install=["git+https://github.com/thuml/iTransformer.git",
                                                          "pandas==1.5.3",
                                                          "scikit-learn==1.2.2",
                                                          "numpy==1.23.5",
                                                          "matplotlib==3.7.0",
                                                          "torch==2.0.0",
                                                          "reformer-pytorch==1.4.4",
                                                          "joblib==1.4.2"])
def train_evaluate_model(
    input_train: Input[Dataset],
    input_val: Input[Dataset],
    input_test: Input[Dataset],
    input_scaler: Input[Model],
    lookback: int, # e.g., last 96 timestamps
    horizon: int, # predict next hour
    output_model: Output[Model],
    evaluation_metrics: Output[Metrics],
):
    import torch
    from torch.utils.data import Dataset, DataLoader
    import pandas as pd
    import joblib

    class TimeSeriesDataset(Dataset):
        def __init__(self, df, lookback, horizon):
            self.X = df.values.astype('float32')
            self.lookback, self.horizon = lookback, horizon

        def __len__(self):
            return len(self.X) - self.lookback - self.horizon + 1

        def __getitem__(self, i):
            x = self.X[i: i + self.lookback]
            y = self.X[i + self.lookback: i + self.lookback + self.horizon]
            return x, y

    X_train = pd.read_csv(input_train.path)
    X_val = pd.read_csv(input_val.path)
    X_test = pd.read_csv(input_test.path)

    feature_names = X_train.columns.tolist()

    train_ds = TimeSeriesDataset(X_train, lookback, horizon)
    val_ds = TimeSeriesDataset(X_val, lookback, horizon)
    test_ds = TimeSeriesDataset(X_test, lookback, horizon)
    train_loader = DataLoader(train_ds, batch_size=32)
    val_loader = DataLoader(val_ds, batch_size=32)
    test_loader = DataLoader(test_ds, batch_size=32)

    from iTransformer import iTransformer

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    model = iTransformer(
        num_variates=X_train.shape[1],
        lookback_len=lookback,
        dim=128,
        depth=4,
        heads=8,
        dim_head=64,
        pred_length=(horizon,),
        num_tokens_per_variate=1,
        use_reversible_instance_norm=True
    ).to(device)

    import torch.nn.functional as F

    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)
    num_epochs = 50

    for epoch in range(num_epochs):
        model.train()
        train_losses = []
        for x, y_true in train_loader:
            x, y_true = x.to(device), y_true.to(device)

            optimizer.zero_grad()

            # Forward pass
            y_pred = model(x)
            # If model(x) returns a dictionary, uncomment this:
            # y_pred = y_pred['pred']  # or the correct key

            # Check shapes
            if y_pred.shape != y_true.shape:
                raise ValueError(f"Shape mismatch: {y_pred.shape} vs {y_true.shape}")

            loss = F.mse_loss(y_pred, y_true)
            loss.backward()
            optimizer.step()
            train_losses.append(loss.item())

        model.eval()
        val_losses = []
        with torch.no_grad():
            for x, y_true in val_loader:
                x, y_true = x.to(device), y_true.to(device)
                y_pred = model(x)
                # If model(x) returns a dict, adjust accordingly:
                # y_pred = y_pred['pred']

                if y_pred.shape != y_true.shape:
                    raise ValueError(f"Validation shape mismatch: {y_pred.shape} vs {y_true.shape}")

                val_losses.append(F.mse_loss(y_pred, y_true).item())

        print(
            f"Epoch {epoch + 1}/{num_epochs}  train_loss: {sum(train_losses) / len(train_losses):.4f}  val_loss: {sum(val_losses) / len(val_losses):.4f}")

    # save the model
    torch.save(model.state_dict(), output_model.path)
    print(f"✅ Model saved to {output_model.path}")


    model.eval()
    all_preds, all_trues = [], []

    with torch.no_grad():
        for x, y_true in test_loader:
            x, y_true = x.to(device), y_true.to(device)
            y_pred = model(x)  # or model(x)['pred'] if it's a dict

            all_preds.append(y_pred.cpu())
            all_trues.append(y_true.cpu())

    # Concatenate batches
    all_preds = torch.cat(all_preds).numpy()  # shape: (num_samples, horizon, num_vars)
    all_trues = torch.cat(all_trues).numpy()

    # Flatten for metrics
    flat_preds = all_preds.reshape(-1, all_preds.shape[-1])
    flat_trues = all_trues.reshape(-1, all_trues.shape[-1])

    # Inverse transform to original scale
    scaler = joblib.load(input_scaler.path)
    flat_preds_original = scaler.inverse_transform(flat_preds)
    flat_trues_original = scaler.inverse_transform(flat_trues)

    from sklearn.metrics import mean_absolute_error, mean_squared_error

    print("Per-variable MAE and MSE:")
    for i, col in enumerate(feature_names):
        mae = mean_absolute_error(flat_trues_original[:, i], flat_preds_original[:, i])
        mse = mean_squared_error(flat_trues_original[:, i], flat_preds_original[:, i])
        print(f"{col}: MAE = {mae:.4f}, MSE = {mse:.4f}")
        evaluation_metrics.log_metric(f"{col}_mae", float(mae))
        evaluation_metrics.log_metric(f"{col}_mse", float(mse))
    overall_mae = mean_absolute_error(flat_trues_original, flat_preds_original)
    overall_mse = mean_squared_error(flat_trues_original, flat_preds_original)
    print(f"Overall: MAE = {overall_mae:.4f}, MSE = {overall_mse:.4f}")
    evaluation_metrics.log_metric("overall_mae", float(overall_mae))
    evaluation_metrics.log_metric("overall_mse", float(overall_mse))
    print("✅ Evaluation metrics logged.")