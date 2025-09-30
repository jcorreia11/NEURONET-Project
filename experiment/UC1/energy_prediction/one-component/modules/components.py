from kfp.dsl import component, Dataset, Model, Metrics, Output

@component(
    base_image="python:3.11",
    packages_to_install=[
        "git+https://github.com/jcorreia11/NEURONET-Project.git",
        "influxdb-client==1.49.0",
        "pandas==2.3.1",
        "click==8.2.1",
        "python-dotenv==1.1.1",
        "scikit-learn==1.7.1",
        "joblib==1.4.2",
        "requests==2.31.0",
    ]
)
def full_pipeline(
    token: str,
    start: str,
    stop: str,
    features: list,
    target: str,
    test_size: float,
    n_estimators: int,
    random_state: int,
    output_x_train: Output[Dataset],
    output_x_test: Output[Dataset],
    output_y_train: Output[Dataset],
    output_y_test: Output[Dataset],
    output_model: Output[Model],
    output_report: Output[Metrics],
):
    """
    Orchestrates the full workflow: get_data → preprocess_data → train_model → evaluate_model
    Saves key artifacts (train/test data, model, report).
    """
    import pandas as pd
    from typing import List, Tuple, Any, Dict

    def get_data_module(token: str, start: str, stop: str):
        def run_query_and_load(token, start, stop, plugin):
            import os
            import glob
            import subprocess
            import pandas as pd
            tmp_dir = f"/tmp/{plugin}_raw"
            os.makedirs(tmp_dir, exist_ok=True)

            # Run influx query, saving all CSVs in tmp_dir
            subprocess.run([
                "query-influxdb",
                "--token", token,
                "--range", f"start: {start}, stop: {stop}",
                "--plugin", plugin,
                "--output-dir", tmp_dir
            ], check=True)

            # Read all CSVs into DataFrames and concatenate
            csv_files = glob.glob(os.path.join(tmp_dir, "*.csv"))
            dfs = [pd.read_csv(csv_file) for csv_file in csv_files]

            if not dfs:
                raise ValueError(f"No CSV files generated for plugin {plugin}")
            else:
                print(f"Loaded {len(dfs)} CSV files for plugin {plugin}")

            df = pd.concat(dfs, ignore_index=True)
            return df

        df_kepler = run_query_and_load(token, start, stop, "kepler")
        df_k8s = run_query_and_load(token, start, stop, "k8s")

        return df_kepler, df_k8s

    def preprocess_data_module(
            kepler_df: pd.DataFrame,
            k8s_df: pd.DataFrame,
            features: List[str],
            target: str,
            test_size: float = 0.2,
            random_state: int = 42,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
        """
        Preprocess Kepler and K8s data, build energy dataset, and split into train/test sets.

        Returns:
            X_train, X_test, y_train, y_test as DataFrames/Series
        """
        from sklearn.model_selection import train_test_split
        from neuronet.datasets.energy_dataset import EnergyDatasetBuilder
        from neuronet.preprocessing.kepler import KeplerPreprocessor
        from neuronet.preprocessing.k8s import K8SProcessor

        # Step 1: Preprocess Kepler data
        kepler_processor = KeplerPreprocessor(kepler_df)
        kepler_df = kepler_processor.run()
        print(f"Kepler processed shape: {kepler_df.shape}")

        # Step 2: Preprocess K8S data
        k8s_processor = K8SProcessor(k8s_df)
        k8s_df = k8s_processor.run()
        print(f"K8S processed shape: {k8s_df.shape}")

        # Step 3: Combine datasets using EnergyDatasetBuilder
        builder = EnergyDatasetBuilder(k8s_df, kepler_df, interval="1min")
        energy_dataset = builder.build()
        print(f"Combined energy dataset shape: {energy_dataset.shape}")

        # Step 4: Split into features and target
        X = energy_dataset[features]
        y = energy_dataset[target]

        # Step 5: Train/test split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state
        )

        print("✅ Preprocessing done. Returning DataFrames instead of saving.")
        return X_train, X_test, y_train, y_test

    def train_model_module(
            X_train: pd.DataFrame,
            y_train: pd.Series,
            n_estimators: int = 100,
            random_state: int = 42,
    ):
        """
        Train a RandomForestRegressor, POST the model file, and return the model object.
        """
        from sklearn.ensemble import RandomForestRegressor
        import joblib
        import tempfile
        import requests
        # Step 1: Train model
        model = RandomForestRegressor(n_estimators=n_estimators, random_state=random_state)
        model.fit(X_train, y_train)
        print("Model trained successfully.")

        # Step 2: Save to temporary local file for API POST
        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp_file:
            joblib.dump(model, tmp_file.name)
            tmp_model_path = tmp_file.name

        # Step 3: POST model
        with open(tmp_model_path, "rb") as f:
            response = requests.post("http://10.255.40.140:30080/model_serializer", files={"file": f})

        # Step 4: Save to KFP artifact
        joblib.dump(model, output_model.path)
        print(f"Model saved to KFP artifact at {output_model.path}")

        print(f"Model POSTed to http://10.255.40.140:30080/model_serializer")
        print(f"POST response status: {response.status_code}")
        print(f"POST response body: {response.text}")

        return model

    def evaluate_model_module(
            X_test: pd.DataFrame,
            y_test: pd.Series,
            model,
    ) -> Dict[str, Any]:
        """
        Evaluate the trained model and return a report with metrics.

        Args:
            X_test: Test feature DataFrame
            y_test: Test target Series
            model: Trained model object (must implement .predict)

        Returns:
            dict with MAE, MSE, R², and predictions
        """
        from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

        # Step 1: Predictions
        y_pred = model.predict(X_test)

        # Step 2: Metrics
        mae = mean_absolute_error(y_test, y_pred)
        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        # Step 3: Build report
        report = {
            "mae": float(mae),
            "mse": float(mse),
            "r2": float(r2),
            "n_samples": len(y_test),
        }

        # Print for visibility
        print("✅ Evaluation complete")
        print(f"MAE: {mae:.4f}")
        print(f"MSE: {mse:.4f}")
        print(f"R² : {r2:.4f}")

        return report

    import json
    import joblib

    # Step 1: Get raw data
    df_kepler, df_k8s = get_data_module(token, start, stop)

    # Remove nas
    df_kepler = df_kepler.dropna()
    df_k8s = df_k8s.dropna()

    print(f"Step 1: Data fetched.")
    print(f"Kepler raw shape: {df_kepler.shape}")
    print(f"K8S raw shape: {df_k8s.shape}")

    # Step 2: Preprocess
    X_train, X_test, y_train, y_test = preprocess_data_module(
        df_kepler, df_k8s, features, target, test_size, random_state
    )

    print(f"Step 2: Data preprocessed.")
    print(f"X_train shape: {X_train.shape}, y_train shape: {y_train.shape}")
    print(f"X_test shape: {X_test.shape}, y_test shape: {y_test.shape}")

    # Save train/test splits as artifacts
    X_train.to_csv(output_x_train.path, index=False)
    X_test.to_csv(output_x_test.path, index=False)
    y_train.to_csv(output_y_train.path, index=False)
    y_test.to_csv(output_y_test.path, index=False)

    # Step 3: Train model
    model = train_model_module(X_train, y_train, n_estimators=n_estimators, random_state=random_state)

    print(f"Step 3: Model trained and saved to {output_model.path}")

    # Step 4: Evaluate model
    report = evaluate_model_module(X_test, y_test, model)

    # Save metrics to KFP UI
    for k, v in report.items():
        if isinstance(v, (int, float)):
            output_report.log_metric(k, float(v))

    # Also save full report as JSON
    report_path = output_report.path + "_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"Step 4: Model evaluated. Report saved to {report_path}")

    print("Full pipeline executed successfully.")
