from kfp.dsl import Input, Output, Dataset, Model, Metrics, component

@component(base_image="python:3.11",
           packages_to_install=["pandas==2.2.2",
                                "torch==2.2.0",
                                "scikit-learn==1.5.2",
                                "joblib==1.4.2"])
def evaluate_model(
    input_x_test: Input[Dataset],
    input_y_test: Input[Dataset],
    input_model: Input[Model],
    evaluation_metrics: Output[Metrics],
):
    import pandas as pd
    import torch
    import torch.nn as nn
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    # --- Load test data ---
    X_test = pd.read_csv(input_x_test.path)
    y_test = pd.read_csv(input_y_test.path).squeeze("columns")

    # --- Load model, scaler, input dimension ---
    checkpoint = torch.load(input_model.path, map_location=torch.device("cpu"))
    scaler = checkpoint['scaler']
    input_dim = checkpoint['input_dim']

    # Normalize features using the saved scaler
    X_test = scaler.transform(X_test)
    X_tensor = torch.tensor(X_test, dtype=torch.float32)

    # Define the same MLP structure as training
    class MLP(nn.Module):
        def __init__(self, input_dim):
            super().__init__()
            self.model = nn.Sequential(
                nn.Linear(input_dim, 128),
                nn.ReLU(),
                nn.Linear(128, 64),
                nn.ReLU(),
                nn.Linear(64, 1)
            )

        def forward(self, x):
            return self.model(x)

    model = MLP(input_dim)
    model.load_state_dict(checkpoint['model_state_dict'])
    model.eval()

    # --- Predict ---
    with torch.no_grad():
        y_pred_tensor = model(X_tensor)
    y_pred = y_pred_tensor.numpy().flatten()

    # --- Calculate metrics ---
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    # --- Log metrics to Kubeflow ---
    evaluation_metrics.log_metric("mae", float(mae))
    evaluation_metrics.log_metric("mse", float(mse))
    evaluation_metrics.log_metric("r2", float(r2))

    # Print them too
    print(f"MAE: {mae:.4f}\nMSE: {mse:.4f}\nR2: {r2:.4f}")
