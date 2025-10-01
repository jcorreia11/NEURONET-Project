import os
import glob
import subprocess
import pandas as pd
from typing import List, Optional, Tuple


def run_query_and_load(token: str, start: str, stop: str, plugin: str) -> pd.DataFrame:
    """Query InfluxDB for a given plugin and load all CSVs into a DataFrame."""
    tmp_dir = f"/tmp/{plugin}_raw"
    os.makedirs(tmp_dir, exist_ok=True)

    subprocess.run([
        "query-influxdb",
        "--token", token,
        "--range", f"start: {start}, stop: {stop}",
        "--plugin", plugin,
        "--output-dir", tmp_dir
    ], check=True)

    csv_files = glob.glob(os.path.join(tmp_dir, "*.csv"))
    dfs = [pd.read_csv(csv_file) for csv_file in csv_files]

    if not dfs:
        raise ValueError(f"No CSV files generated for plugin {plugin}")

    return pd.concat(dfs, ignore_index=True)


def get_data(token: str, start: str, stop: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch Kepler and K8s data within a timeframe."""
    df_kepler = run_query_and_load(token, start, stop, "kepler").dropna()
    df_k8s = run_query_and_load(token, start, stop, "k8s").dropna()
    return df_kepler, df_k8s


def preprocess_data(
    df_kepler: pd.DataFrame,
    df_k8s: pd.DataFrame,
    features: List[str],
    target: Optional[str] = None,
) -> Tuple[pd.DataFrame, Optional[pd.Series]]:
    """Preprocess Kepler+K8s into a feature matrix (and target if available)."""
    from neuronet.datasets.energy_dataset import EnergyDatasetBuilder
    from neuronet.preprocessing.kepler import KeplerPreprocessor
    from neuronet.preprocessing.k8s import K8SProcessor

    # Preprocess
    kepler_proc = KeplerPreprocessor(df_kepler)
    df_kepler = kepler_proc.run()

    k8s_proc = K8SProcessor(df_k8s)
    df_k8s = k8s_proc.run()

    # Combine into energy dataset
    builder = EnergyDatasetBuilder(k8s_df=df_k8s, kepler_df=df_kepler, interval="1min")
    dataset = builder.build()

    # Extract features (and target if requested)
    X = dataset[features]
    y = dataset[target] if target and target in dataset.columns else None

    print(f"Preprocessing done. X shape: {X.shape}")
    return X, y


if __name__ == "__main__":
    TOKEN = "AK35JfgefqFN96DGYjoprx6M-UFk05AZDIAeizRVeINXiD42CVExfZ_afXuLvCSFHvyOZS6MWPVz8itjfEkKzQ=="
    START = "2025-08-23T19:10:50Z"
    STOP = "2025-08-23T19:20:50Z"
    FEATURES = ["cpu_millicores","memory_usage_mb","logsfs_usage_percent"]
    TARGET = "container_power_watts"

    # Step 1: Get raw data
    df_kepler, df_k8s = get_data(TOKEN, START, STOP)

    # Step 2: Preprocess to features
    X, y = preprocess_data(df_kepler, df_k8s, FEATURES, target=TARGET)

    # Save to CSV if desired
    X.to_csv("X_new.csv", index=False)
    if y is not None:
        y.to_csv("y_new.csv", index=False)

    print("Data ready to feed the model.")

