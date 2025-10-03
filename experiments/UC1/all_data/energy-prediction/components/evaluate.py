from kfp.dsl import component

@component(
    base_image="python:3.11",
    packages_to_install=["pandas==2.3.1", "scikit-learn==1.7.1", "joblib==1.4.2"]
)
def evaluate_model(
    data_dir: str,
    target: str,
):
    """
    Evaluates a trained model on the test set and prints MAE, MSE, and R2.
    Fully local; no MinIO/artifact outputs.
    """
    import pandas as pd
    import joblib
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    import os

    # Load test data
    test_data = pd.read_csv(os.path.join(data_dir, "test_data.csv"))
    X_test = test_data.drop(columns=[target])
    y_test = test_data[target]  # Series is fine

    # Load trained model
    model = joblib.load(os.path.join(data_dir, "model.pkl"))

    # Predict and calculate metrics
    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    # Print metrics
    print(f"✅ Model evaluation results:")
    print(f"MAE: {mae:.4f}")
    print(f"MSE: {mse:.4f}")
    print(f"R2:  {r2:.4f}")
