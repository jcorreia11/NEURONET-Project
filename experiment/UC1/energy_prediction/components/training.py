from kfp.dsl import Input, Output, Dataset, Model, component

@component(base_image="python:3.11", packages_to_install=["pandas==2.2.2","scikit-learn==1.5.2","joblib==1.4.2"])
def train_model(
    input_x_train: Input[Dataset],
    input_y_train: Input[Dataset],
    n_estimators: int,
    random_state: int,
    output_model: Output[Model],
):
    import pandas as pd
    from sklearn.ensemble import RandomForestRegressor
    import joblib
    import os
    X_train = pd.read_csv(input_x_train.path)
    y_train = pd.read_csv(input_y_train.path).squeeze("columns")

    model = RandomForestRegressor(n_estimators=n_estimators, random_state=random_state)
    model.fit(X_train, y_train)

    os.makedirs(os.path.dirname(output_model.path), exist_ok=True)
    joblib.dump(model, output_model.path)
    print(f"Model saved to {output_model.path}")
