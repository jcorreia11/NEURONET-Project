from kfp.dsl import Input, Output, Dataset, Model, Metrics, component

@component(base_image="python:3.11", packages_to_install=["pandas==2.2.2","scikit-learn==1.5.2","joblib==1.4.2"])
def evaluate_model(
    input_x_test: Input[Dataset],
    input_y_test: Input[Dataset],
    input_model: Input[Model],
    evaluation_metrics: Output[Metrics],
):
    import pandas as pd
    import joblib
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    X_test = pd.read_csv(input_x_test.path)
    y_test = pd.read_csv(input_y_test.path).squeeze("columns")
    model = joblib.load(input_model.path)

    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    # Log metrics so they appear in KFP UI
    evaluation_metrics.log_metric("mae", float(mae))
    evaluation_metrics.log_metric("mse", float(mse))
    evaluation_metrics.log_metric("r2", float(r2))

    # Also print them
    print(f"MAE: {mae:.4f}\nMSE: {mse:.4f}\nR2: {r2:.4f}")
