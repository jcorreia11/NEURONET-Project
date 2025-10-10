from kfp.dsl import component, Input, Output, Dataset, Model

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
    train_data: Input[Dataset],
    model_file: Output[Model],
    params_file: Output[Dataset],
    target: str,
    random_state: int = 42,
    usecase: str = "energy_prediction",
    post_base_url: str = "http://fastapi-model-svc.admin.svc.cluster.local:8080",
):
    import pandas as pd
    import joblib
    import json
    import requests
    from sklearn.model_selection import RandomizedSearchCV, ParameterGrid
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.svm import SVR
    from sklearn.linear_model import LinearRegression
    from xgboost import XGBRegressor

    print("⏳ Optimize and train model component started.")

    print("Loading training data from:", train_data.path)

    train_df = pd.read_csv(train_data.path)
    X_train = train_df.drop(columns=[target])
    y_train = train_df[target].values.ravel()

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
        model = m["model"]
        param_grid = m["params"]

        if param_grid:
            n_iter = min(10, len(list(ParameterGrid(param_grid))))
            search = RandomizedSearchCV(model, param_grid, n_iter=n_iter, cv=3,
                                        scoring="r2", n_jobs=-1, random_state=random_state)
            search.fit(X_train, y_train)
            score = search.best_score_
            params = search.best_params_
        else:
            model.fit(X_train, y_train)
            score = model.score(X_train, y_train)
            params = {}

        if score > best_score:
            best_score = score
            best_model_name = name
            best_params = params

    # Retrain best model on all data
    if best_model_name == "RandomForest":
        final_model = RandomForestRegressor(random_state=random_state, **best_params)
    elif best_model_name == "XGBoost":
        final_model = XGBRegressor(random_state=random_state, eval_metric="rmse", **best_params)
    elif best_model_name == "SVM":
        final_model = SVR(**best_params)
    else:
        final_model = LinearRegression()

    final_model.fit(X_train, y_train)

    # Save model and params to artifacts
    joblib.dump(final_model, model_file.path)
    with open(params_file.path, "w") as f:
        json.dump({"model": best_model_name, "parameters": best_params}, f, indent=2)

    # BASE = "http://10.255.40.149:30080/"
    # BASE = "http://fastapi-model-svc.admin.svc.cluster.local:8080"
    BASE = post_base_url
    model_file_path = model_file.path

    post_url = f"{BASE}/model_serializer/{usecase}"

    # Also save as pkl file for POST
    joblib.dump(final_model, "model.pkl")

    with open("model.pkl", "rb") as f:
        response = requests.post(post_url, files={"file": f})

    if response.status_code == 200:
        print(f"✅ Model uploaded successfully for usecase '{usecase}'")
        print("Response:", response.json())
    else:
        print(f"❌ Failed to upload model: {response.status_code}")
        print("Response:", response.text)

    print(f"Training component finished.")
