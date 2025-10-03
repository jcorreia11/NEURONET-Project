from kfp.dsl import component
from typing import List


@component(
    base_image="python:3.11",
    packages_to_install=[
        "pandas==2.3.1",
        "scikit-learn==1.7.1"
    ]
)
def preprocess_data(
        data_dir: str,
        features: List[str],
        target: str,
        test_size: float,
        random_state: int,
):
    import os
    import pandas as pd
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler
    import joblib

    # ensure output dir exists
    os.makedirs(data_dir, exist_ok=True)

    # load dataset
    data = pd.read_csv(os.path.join(data_dir, "dataset_all_data.csv"))

    X = data[features]
    y = data[target]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )

    # scale features
    scaler = StandardScaler()
    X_train = pd.DataFrame(scaler.fit_transform(X_train), columns=features)
    X_test = pd.DataFrame(scaler.transform(X_test), columns=features)

    # Merge X and y for saving (columns: features + target)
    train_data = pd.concat([X_train, y_train.reset_index(drop=True)], axis=1)
    test_data = pd.concat([X_test, y_test.reset_index(drop=True)], axis=1)
    train_data.to_csv(os.path.join(data_dir, "train_data.csv"), index=False)
    test_data.to_csv(os.path.join(data_dir, "test_data.csv"), index=False)

    #  save scaler
    joblib.dump(scaler, os.path.join(data_dir, "scaler.pkl"))

    print("✅ Preprocessing done. CSVs saved locally.")


