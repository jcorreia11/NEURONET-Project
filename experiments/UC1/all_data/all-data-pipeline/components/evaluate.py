from kfp.dsl import component, Input, Dataset, Model, Output, Metrics

@component(
    base_image="python:3.11",
    packages_to_install=[
        "pandas==2.3.1",
        "scikit-learn==1.7.1",
        "joblib==1.4.2",
        "fsspec==2025.9.0",
    ]
)
def evaluate_model(
    test_data: Input[Dataset],
    model: Input[Model],
    metrics: Output[Metrics],
):
    """
    Evaluates a trained model on the test set and writes metrics to KFP metrics file.
    Fully local; no MinIO needed.
    """
    import pandas as pd
    import joblib
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    #import fsspec

    print("⏳ Evaluate model component started.")

    print("Loading test data from:", test_data.path)
    # Load test data
    # Download MinIO artifact to local path
    # with fsspec.open(test_data.path, 'r') as f:
    #     test_df = pd.read_csv(f)
    test_df = pd.read_csv(test_data.path)
    target_col = test_df.columns[-1]  # assume last column is target, or pass as param
    X_test = test_df.drop(columns=[target_col])
    y_test = test_df[target_col]

    # Load trained model
    # with fsspec.open(model.path, 'r') as f:
    #     trained_model = joblib.load(f)
    trained_model = joblib.load(model.path)

    # Predict
    y_pred = trained_model.predict(X_test)

    # Compute metrics
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    # Print metrics
    print(f"✅ Model evaluation results:")
    print(f"MAE: {mae:.4f}")
    print(f"MSE: {mse:.4f}")
    print(f"R2:  {r2:.4f}")

    # Write metrics to KFP metrics JSON
    metrics.log_metric("mae", mae)
    metrics.log_metric("mse", mse)
    metrics.log_metric("r2", r2)
    metrics.log_metric("n_samples", len(y_test))

    print(f"✅ Metrics written to {metrics.path}")
    print("✅ Evaluate model component finished.")
