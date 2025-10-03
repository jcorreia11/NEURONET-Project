from kfp.dsl import component

@component(
    base_image="python:3.11",
    packages_to_install=[
        "pandas==2.3.1",
        "scikit-learn==1.7.1",
        "xgboost==1.7.5",
        "joblib==1.4.2",
        "requests==2.31.0",
    ]
)
def optimize_model_hyperparameters_and_train(
    data_dir: str,
    target: str,
    random_state: int = 42
):
    import pandas as pd
    from sklearn.model_selection import RandomizedSearchCV, ParameterGrid
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.svm import SVR
    from sklearn.linear_model import LinearRegression
    from xgboost import XGBRegressor
    import joblib
    import json
    import os
    import requests

    # Load training data
    train_data = pd.read_csv(os.path.join(data_dir, "train_data.csv"))
    X_train = train_data.drop(columns=[target])
    y_train = train_data[target].values.ravel()

    models = {
        "RandomForest": {"model": RandomForestRegressor(random_state=random_state),
                         "params": {"n_estimators": [50, 100], "max_depth": [None, 5, 10]}},
        "XGBoost": {"model": XGBRegressor(random_state=random_state, eval_metric="rmse"),
                    "params": {"n_estimators": [50, 100], "max_depth": [3, 5, 7], "learning_rate": [0.01, 0.1]}},
        "SVM": {"model": SVR(), "params": {"C": [0.1, 1, 10], "kernel": ["linear", "rbf"]}},
        "LinearRegression": {"model": LinearRegression(), "params": {}}
    }

    best_score = float("-inf")
    best_model_name = None
    best_params = None

    # Hyperparameter search
    for name, m in models.items():
        print(f"Optimizing {name}...")
        model = m["model"]
        param_grid = m["params"]

        if param_grid:
            n_iter = min(10, len(list(ParameterGrid(param_grid))))
            search = RandomizedSearchCV(model, param_grid, n_iter=n_iter, cv=3, scoring="r2", n_jobs=-1, random_state=random_state)
            search.fit(X_train, y_train)
            score = search.best_score_
            params = search.best_params_
        else:
            model.fit(X_train, y_train)
            score = model.score(X_train, y_train)
            params = {}

        print(f"{name} R2 score: {score:.4f}")

        if score > best_score:
            best_score = score
            best_model_name = name
            best_params = params

    print(f"✅ Best model: {best_model_name} with CV R2={best_score:.4f}")

    # Retrain best model on all data
    if best_model_name == "RandomForest":
        final_model = RandomForestRegressor(random_state=random_state, **best_params)
    elif best_model_name == "XGBoost":
        final_model = XGBRegressor(random_state=random_state, eval_metric="rmse", **best_params)
    elif best_model_name == "SVM":
        final_model = SVR(**best_params)
    else:  # LinearRegression
        final_model = LinearRegression()

    final_model.fit(X_train, y_train)

    # Save model and params
    model_path = os.path.join(data_dir, "model.pkl")
    params_path = os.path.join(data_dir, "best_params.json")
    joblib.dump(final_model, model_path)
    with open(params_path, "w") as f:
        json.dump({"model": best_model_name, "parameters": best_params}, f, indent=2)

    print(f"✅ Retrained model saved to: {model_path}")
    print(f"✅ Best parameters saved to: {params_path}")

    # POST model file to API
    post_url = "http://10.255.40.140:30080/model_serializer"
    with open(model_path, "rb") as f:
        response = requests.post(post_url, files={"file": f})

    print(f"POST response status: {response.status_code}")
    print(f"POST response body: {response.text}")


