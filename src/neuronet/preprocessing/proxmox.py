import os
import pandas as pd
from typing import List


class ProxmoxDataProcessor:
    def __init__(self, directory: str):
        self.directory = directory
        self.dataframes: List[pd.DataFrame] = []
        self.final_df: pd.DataFrame = pd.DataFrame()

    def load_files(self):
        """Load all CSV files starting with 'proxmox' from the directory."""
        for filename in os.listdir(self.directory):
            if filename.startswith("proxmox") and filename.endswith(".csv"):
                file_path = os.path.join(self.directory, filename)
                df = pd.read_csv(file_path)
                self.dataframes.append(df)

    def process_dataframes(self):
        """Process and pivot each raw Proxmox dataframe, then merge."""
        processed_dfs = []

        for df in self.dataframes:
            df['_time'] = pd.to_datetime(df['_time'])

            df['_value'] = pd.to_numeric(df['_value'], errors='coerce')

            # Pivot based on time and measurement field
            df_pivoted = df.pivot_table(
                index=['_time', 'inventory-server-id', 'vm_id', 'vm_name'],
                columns='_field',
                values='_value',
                aggfunc='mean'
            ).reset_index()

            # Convert disk metrics from bytes to GB
            if 'disk_free' in df_pivoted.columns:
                df_pivoted['disk_free_gb'] = df_pivoted['disk_free'] / (1024**3)
            if 'disk_total' in df_pivoted.columns:
                df_pivoted['disk_total_gb'] = df_pivoted['disk_total'] / (1024**3)
            if 'disk_free' in df_pivoted.columns and 'disk_total' in df_pivoted.columns:
                df_pivoted['disk_used_gb'] = (df_pivoted['disk_total'] - df_pivoted['disk_free']) / (1024**3)
                df_pivoted['disk_usage_percent'] = 100 * (1 - df_pivoted['disk_free'] / df_pivoted['disk_total'])

            processed_dfs.append(df_pivoted)

        # Merge all processed frames
        self.final_df = pd.concat(processed_dfs, ignore_index=True).sort_values('_time')

    def save_to_csv(self, output_path: str):
        """Save the final processed dataset to a CSV file."""
        self.final_df.to_csv(output_path, index=False)

    def run(self, output_csv: str = "proxmox_processed.csv"):
        """Main execution method."""
        self.load_files()
        self.process_dataframes()
        os.makedirs(os.path.join(self.directory, 'processed'), exist_ok=True)
        self.save_to_csv(os.path.join(self.directory, 'processed', output_csv))
        print(f"✅ Processed data saved to: {os.path.join(self.directory, 'processed', output_csv)}")


if __name__ == "__main__":
    # Example usage
    processor = ProxmoxDataProcessor(directory="experiment/")
    processor.run(output_csv="proxmox_processed.csv")
    print("Proxmox data processing complete.")
