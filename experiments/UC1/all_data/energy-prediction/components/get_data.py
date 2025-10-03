from kfp.dsl import component

@component(
    base_image="python:3.11",
    packages_to_install=[
        "pandas==2.3.1",
    ]
)
def get_data(output_dir: str):
    """
    Downloads dataset_all_data.csv from GitHub
    and saves it into the given output directory.
    """
    import os
    import pandas as pd

    # URL to dataset
    url = "https://raw.githubusercontent.com/jcorreia11/NEURONET-Project/main/experiments/UC1/all_data/dataset_all_data.csv"
    df = pd.read_csv(url)

    # Show preview
    print("✅ Data preview:")
    print(df.head())

    # Save locally
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, "dataset_all_data.csv")
    df.to_csv(csv_path, index=False)
    print(f"✅ CSV saved to {csv_path}")

