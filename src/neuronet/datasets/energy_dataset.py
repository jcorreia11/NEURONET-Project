import pandas as pd

class EnergyDatasetBuilder:
    def __init__(self, k8s_df: pd.DataFrame, kepler_df: pd.DataFrame, interval: str = '1min'):
        self.k8s_df = k8s_df.copy()
        self.kepler_df = kepler_df.copy()
        self.interval = interval
        self.dataset = None

    def preprocess_time(self):
        # Align all times to the given interval (e.g., 1min)
        for df in [self.k8s_df, self.kepler_df]:
            df['_time'] = pd.to_datetime(df['_time'])
            df['_time'] = df['_time'].dt.floor(self.interval)

    def aggregate_kepler(self):
        """Group Kepler by _time + container_name + namespace + pod_name and sum joules."""
        kepler_grouped = (
            self.kepler_df
            .groupby(['_time', 'container_name', 'namespace', 'pod_name'], as_index=False)
            .agg({'kepler_container_joules_total': 'sum'})
        )
        self.kepler_df = kepler_grouped

    def join_data(self):
        # Filter only required columns to reduce memory
        k8s_cols = [
            '_time', 'container_name', 'namespace', 'pod_name',
            'cpu_usage_nanocores', 'memory_usage_bytes',
            'logsfs_used_bytes', 'logsfs_capacity_bytes'
        ]
        kepler_cols = ['_time', 'container_name', 'namespace', 'pod_name', 'kepler_container_joules_total']

        k8s_filtered = self.k8s_df[k8s_cols].dropna()
        kepler_filtered = self.kepler_df[kepler_cols].dropna()

        # Merge on multiple keys
        self.dataset = pd.merge(
            k8s_filtered,
            kepler_filtered,
            on=['_time', 'container_name', 'namespace', 'pod_name'],
            how='inner'
        )

    def engineer_features(self):
        df = self.dataset

        # Unit conversions
        df['cpu_millicores'] = df['cpu_usage_nanocores'] / 1e6
        df['memory_usage_mb'] = df['memory_usage_bytes'] / (1024 ** 2)

        # Avoid divide-by-zero
        df['logsfs_usage_percent'] = df.apply(
            lambda x: (x['logsfs_used_bytes'] / x['logsfs_capacity_bytes'] * 100)
            if x['logsfs_capacity_bytes'] > 0 else 0,
            axis=1
        )

        # Target: power in watts = joules per 60s interval
        df['container_power_watts'] = df['kepler_container_joules_total'] / 60

        # Final dataset
        self.dataset = df[[
            '_time', 'container_name', 'namespace', 'pod_name',
            'cpu_millicores', 'memory_usage_mb', 'logsfs_usage_percent',
            'container_power_watts'
        ]].dropna()

    def build(self) -> pd.DataFrame:
        self.preprocess_time()
        self.aggregate_kepler()
        self.join_data()
        self.engineer_features()
        return self.dataset

if __name__ == "__main__":
    # Example usage
    k8s_df = pd.read_csv('experiment/processed/k8s_processed.csv')  # Load your K8s data
    kepler_df = pd.read_csv('experiment/processed/kepler_processed.csv')  # Load your Kepler data

    builder = EnergyDatasetBuilder(k8s_df, kepler_df, interval='1min')
    energy_dataset = builder.build()
    energy_dataset.to_csv('experiment/datasets/energy_dataset.csv', index=False)
    print("Energy dataset built and saved to 'energy_dataset.csv'")