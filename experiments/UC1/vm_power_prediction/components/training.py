from kfp.dsl import Input, Output, Dataset, Model, component

@component(base_image="python:3.11",
           packages_to_install=["pandas==2.2.2",
                                "torch==2.2.0",
                                "scikit-learn==1.5.2",
                                "joblib==1.4.2"])
def train_model(
    input_x_train: Input[Dataset],
    input_y_train: Input[Dataset],
    epochs: int,
    lr: float,
    output_model: Output[Model],
):
    import os
    import pandas as pd
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from sklearn.preprocessing import StandardScaler
    from torch.utils.data import DataLoader, TensorDataset

    # --- Load data ---
    X_train = pd.read_csv(input_x_train.path)
    y_train = pd.read_csv(input_y_train.path).squeeze("columns")

    # --- Normalize features ---
    scaler = StandardScaler()
    X_train = scaler.fit_transform(X_train)

    # --- Convert to tensors ---
    X_tensor = torch.tensor(X_train, dtype=torch.float32)
    y_tensor = torch.tensor(y_train.values, dtype=torch.float32).view(-1, 1)

    # --- Create dataset and loader ---
    dataset = TensorDataset(X_tensor, y_tensor)
    loader = DataLoader(dataset, batch_size=32, shuffle=True)

    # --- Define a simple MLP ---
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

    model = MLP(input_dim=X_tensor.shape[1])
    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=lr)

    # --- Train loop ---
    model.train()
    for epoch in range(epochs):
        running_loss = 0.0
        for batch_x, batch_y in loader:
            optimizer.zero_grad()
            outputs = model(batch_x)
            loss = criterion(outputs, batch_y)
            loss.backward()
            optimizer.step()
            running_loss += loss.item() * batch_x.size(0)
        epoch_loss = running_loss / len(loader.dataset)
        print(f"Epoch [{epoch + 1}/{epochs}], Loss: {epoch_loss:.4f}")

    # --- Save model ---
    os.makedirs(os.path.dirname(output_model.path), exist_ok=True)
    torch.save({
        'model_state_dict': model.state_dict(),
        'scaler': scaler,
        'input_dim': X_tensor.shape[1]
    }, output_model.path)

    print(f"Model saved to {output_model.path}")
