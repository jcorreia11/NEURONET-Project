import pandas as pd

class VmPowerDatasetBuilder:
    def __init__(self, proxmox_df: pd.DataFrame, scaphandre_vm_df: pd.DataFrame, interval='1min'):
        self.proxmox_df = proxmox_df.copy()
        # keep only k8s VMs
        self.proxmox_df = self.proxmox_df[self.proxmox_df['vm_name'].str.contains('k8s', na=False)]
        self.scaphandre_vm_df = scaphandre_vm_df.copy()
        # keep only k8s VMs
        self.scaphandre_vm_df = self.scaphandre_vm_df[self.scaphandre_vm_df['vm_name'].str.contains('k8s', na=False)]
        self.interval = interval
        self.dataset = None

    def preprocess_time(self):
        for df in [self.proxmox_df, self.scaphandre_vm_df]:
            df['_time'] = pd.to_datetime(df['_time'])
            df['_time'] = df['_time'].dt.floor(self.interval)

    def aggregate_scaphandre(self):
        # Aggregate all relevant Scaphandre numeric columns by _time and vm_id
        agg_cols = {
            'scaph_process_cpu_usage_percentage': 'mean',
            'scaph_process_disk_read_bytes': 'sum',
            'scaph_process_disk_total_read_bytes': 'sum',
            'scaph_process_disk_total_write_bytes': 'sum',
            'scaph_process_disk_write_bytes': 'sum',
            'scaph_process_memory_bytes': 'mean',
            'scaph_process_memory_virtual_bytes': 'mean',
            'scaph_process_power_consumption_microwatts': 'sum'
        }
        self.scaphandre_vm_df = (
            self.scaphandre_vm_df
            .groupby(['_time', 'vm_id'], as_index=False)
            .agg(agg_cols)
        )

    def join_data(self):
        # Select relevant Proxmox columns — all numeric plus vm_id and _time
        prox_cols = [
            '_time', 'vm_id', 'cpuload', 'disk_free', 'disk_total', 'disk_used', 'disk_used_percentage',
            'mem_free', 'mem_total', 'mem_used', 'mem_used_percentage',
            'swap_free', 'swap_total', 'swap_used', 'swap_used_percentage',
            'uptime', 'disk_free_gb', 'disk_total_gb', 'disk_used_gb', 'disk_usage_percent'
        ]
        prox_clean = self.proxmox_df[prox_cols].dropna()

        # Merge
        merged = pd.merge(prox_clean, self.scaphandre_vm_df, on=['_time', 'vm_id'], how='inner')

        self.dataset = merged

    def engineer_features(self):
        df = self.dataset.copy()

        # Convert uptime seconds to hours
        df['uptime_hours'] = df['uptime'] / 3600

        # Convert power from microwatts to watts
        df['vm_power_watts'] = df['scaph_process_power_consumption_microwatts'] / 1e6

        # Add memory usage percentage if missing or double check
        if 'mem_used_percentage' not in df.columns:
            df['mem_used_percentage'] = df['mem_used'] / df['mem_total'] * 100

        # Add swap usage percentage if missing or double check
        if 'swap_used_percentage' not in df.columns and 'swap_used' in df.columns and 'swap_total' in df.columns:
            df['swap_used_percentage'] = df['swap_used'] / df['swap_total'] * 100

        # Final selection: keep all columns relevant for modeling or analysis
        # Keep _time and vm_id for reference, all Proxmox numeric cols, all Scaphandre numeric cols, and engineered features
        self.dataset = df.dropna()

    def build(self) -> pd.DataFrame:
        self.preprocess_time()
        self.aggregate_scaphandre()
        self.join_data()
        self.engineer_features()
        return self.dataset

if __name__ == "__main__":
    # Example usage
    proxmox_df = pd.read_csv("experiment/processed/proxmox_processed.csv")  # Load your Proxmox data
    scaphandre_vm_df = pd.read_csv("experiment/processed/vm_scaphandre_processed.csv")  # Load your Scaphandre VM data

    builder = VmPowerDatasetBuilder(proxmox_df, scaphandre_vm_df)
    final_dataset = builder.build()
    final_dataset.to_csv("experiment/datasets/vm_power_dataset.csv", index=False)
    print("✅ VM Power Dataset built and saved to vm_power_dataset.csv")