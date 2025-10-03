from kfp.dsl import component, Input, Output, Dataset, Model
from typing import List

@component(
    base_image="python:3.11",
    packages_to_install=[
        "pandas==2.3.1",
        "scikit-learn==1.7.1",
        "joblib"
    ]
)
def preprocess_data(
    df: Input[Dataset],
    train_data: Output[Dataset],
    test_data: Output[Dataset],
    scaler_file: Output[Model],
    features: List[str],
    target: str,
    test_size: float,
    random_state: int,
):
    import pandas as pd
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler
    import joblib

    print("⏳ Preprocess data component started.")

    # ✅ Load dataset from local path (KFP downloads automatically)
    data = pd.read_csv(df.path)

    print("✅ Data preview:")
    print(data.head())

    # remove rows with missing values (on the features and target columns)
    print(f"✅ Data shape before dropping missing values: {data.shape}")
    data = data.dropna(subset=features + [target])
    print(f"✅ Data shape after dropping missing values: {data.shape}")

    X = data[features]
    y = data[target]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )

    # scale features
    scaler = StandardScaler()
    X_train_scaled = pd.DataFrame(scaler.fit_transform(X_train), columns=features)
    X_test_scaled = pd.DataFrame(scaler.transform(X_test), columns=features)

    # merge X and y
    train_df = pd.concat([X_train_scaled, y_train.reset_index(drop=True)], axis=1)
    test_df = pd.concat([X_test_scaled, y_test.reset_index(drop=True)], axis=1)

    # ✅ Save to artifact paths (KFP uploads them to MinIO)
    train_df.to_csv(train_data.path, index=False)
    test_df.to_csv(test_data.path, index=False)
    joblib.dump(scaler, scaler_file.path)

    print("✅ Preprocessing done. Artifacts saved.")
    print("Train data saved to:", train_data.path)
    print("Test data saved to:", test_data.path)
    print("Scaler saved to:", scaler_file.path)

    print("✅ Preprocess data component finished.")
