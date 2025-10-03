from kfp.dsl import component, Output, Dataset

@component(
    base_image="python:3.11",
    packages_to_install=["pandas==2.3.1"]
)
def get_data(data_csv: Output[Dataset]):
    """
    Downloads dataset_all_data.csv from GitHub
    and saves it as an output Dataset artifact.
    """
    import pandas as pd

    print("⏳ Get data component started.")

    # URL to dataset
    url = "https://raw.githubusercontent.com/jcorreia11/NEURONET-Project/main/experiments/UC1/all_data/dataset_all_data.csv"
    df = pd.read_csv(url)

    # Show preview
    print("✅ Data preview:")
    print(df.head())

    # Save to artifact location (KFP handles MinIO upload)
    df.to_csv(data_csv.path, index=False)
    print("Data preview:")
    print(df.head())
    print(f"✅ CSV saved to {data_csv.path}")
    print("✅ Get data component finished.")
