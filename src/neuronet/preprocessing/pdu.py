import os
import pandas as pd
from typing import List


class PDUDataProcessor:
    def __init__(self, directory: str):
        self.directory = directory
        self.dataframes: List[pd.DataFrame] = []
        self.final_df: pd.DataFrame = pd.DataFrame()

    def load_files(self):
        """Load all CSV files starting with 'pdu' from the directory."""
        for filename in os.listdir(self.directory):
            if filename.startswith("pdu") and filename.endswith(".csv"):
                file_path = os.path.join(self.directory, filename)
                df = pd.read_csv(file_path)
                self.dataframes.append(df)

    def process_dataframes(self):
        """Process and pivot each raw PDU dataframe, then merge."""
        processed_dfs = []

        for df in self.dataframes:
            # Ensure proper timestamp format
            df['_time'] = pd.to_datetime(df['_time'])

            # Pivot based on time and measurement field
            df_pivoted = df.pivot_table(
                index=['_time', 'inventory-server-id', 'placement', 'url'],
                columns='_field',
                values='_value',
                aggfunc='mean'  # in case of duplicate rows
            ).reset_index()

            # Add URL or other metadata if desired
            if 'url' in df.columns:
                meta = df[['inventory-server-id', 'url']].drop_duplicates()
                df_pivoted = df_pivoted.merge(meta, on='inventory-server-id', how='left')

            processed_dfs.append(df_pivoted)

        # Concatenate all processed frames
        self.final_df = pd.concat(processed_dfs, ignore_index=True).sort_values('_time')

    def save_to_csv(self, output_path: str):
        """Save the final processed dataset to a CSV file."""
        self.final_df.to_csv(output_path, index=False)

    def run(self, output_csv: str = "pdu_processed.csv"):
        """Main execution method."""
        self.load_files()
        self.process_dataframes()
        os.makedirs(os.path.join(self.directory, 'processed'), exist_ok=True)
        self.save_to_csv(os.path.join(self.directory, 'processed', output_csv))
        print(f"✅ Processed data saved to: {os.path.join(self.directory, output_csv)}")

if __name__ == "__main__":
    # Example usage
    processor = PDUDataProcessor(directory="experiment/")
    processor.run(output_csv="pdu_processed.csv")
    print("PDU data processing complete.")