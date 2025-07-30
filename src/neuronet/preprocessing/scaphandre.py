import os
from typing import List

import pandas as pd


class ScaphandreProcessor:
    def __init__(self, directory: str):
        self.directory: str = directory
        self.dataframes_host: List[pd.DataFrame] = []
        self.dataframes_vm: List[pd.DataFrame] = []
        self.final_df_host: pd.DataFrame = pd.DataFrame()
        self.final_df_vms: pd.DataFrame = pd.DataFrame()

    def load_files(self):
        """Load all CSV files starting with 'scaphandre' from the directory."""
        for filename in os.listdir(self.directory):
            if filename.startswith("scaphandre_flux") and filename.endswith(".csv"):
                file_path = os.path.join(self.directory, filename)
                df = pd.read_csv(file_path)
                self.dataframes_host.append(df)
            elif filename.startswith("scaphandre_neuronet") and filename.endswith(".csv"):
                file_path = os.path.join(self.directory, filename)
                df = pd.read_csv(file_path)
                self.dataframes_vm.append(df)

    def process_dataframes(self):
        """Process and pivot each raw Scaphandre dataframe, then merge."""
        processed_dfs_host = []
        processed_dfs_vm = []

        for df in self.dataframes_host:
            # Ensure proper timestamp format
            df['_time'] = pd.to_datetime(df['_time'])

            # Pivot based on time and measurement field
            df_pivoted = df.pivot_table(
                index=['_time', 'inventory-cluster-id', 'inventory-rack-id', 'url'],
                columns='_field',
                values='_value',
                aggfunc='mean'  # in case of duplicate rows
            ).reset_index()

            processed_dfs_host.append(df_pivoted)

        for df in self.dataframes_vm:
            # Ensure proper timestamp format
            df['_time'] = pd.to_datetime(df['_time'])

            # Pivot based on time and measurement field
            df_pivoted = df.pivot_table(
                index=['_time', 'inventory-cluster-id', 'inventory-rack-id', 'vm_id', 'vm_name'],
                columns='_field',
                values='_value',
                aggfunc='mean'  # in case of duplicate rows
            ).reset_index()

            processed_dfs_vm.append(df_pivoted)

        # Concatenate all processed frames
        self.final_df_host = pd.concat(processed_dfs_host, ignore_index=True).sort_values('_time')
        self.final_df_vms = pd.concat(processed_dfs_vm, ignore_index=True).sort_values('_time')

    def save_to_csv(self, output_path: str = "scaphandre_processed.csv"):
        """Save the final processed datasets to CSV files."""
        host_path = os.path.join(self.directory, 'processed', f'host_{output_path}')
        vm_path = os.path.join(self.directory, 'processed', f'vm_{output_path}')

        self.final_df_host.to_csv(host_path, index=False)
        self.final_df_vms.to_csv(vm_path, index=False)

    def run(self, output_csv: str = "scaphandre_processed.csv"):
        """Main execution method."""
        self.load_files()
        self.process_dataframes()
        os.makedirs(os.path.join(self.directory, 'processed'), exist_ok=True)
        self.save_to_csv(output_csv)
        print(f"✅ Processed data saved to: {os.path.join(self.directory, 'processed', output_csv)}")