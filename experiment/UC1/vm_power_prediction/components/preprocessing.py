from kfp.dsl import Output, Input, Dataset, component
from typing import List


@component(base_image="python:3.11",
           packages_to_install=[
               "git+https://github.com/jcorreia11/NEURONET-Project.git",
               "pandas==2.3.1",
               "scikit-learn==1.7.1"])
def preprocess_data(
    input_proxmox_dir: Input[Dataset],
    input_scaphandre_dir: Input[Dataset],
    features: List[str],
    target: str,
    test_size: float,
    random_state: int,
    output_x_train: Output[Dataset],
    output_x_test: Output[Dataset],
    output_y_train: Output[Dataset],
    output_y_test: Output[Dataset],
):
    import pandas as pd
    import os
    from sklearn.model_selection import train_test_split
    from neuronet.datasets.vm_power_dataset import VmPowerDatasetBuilder

    # Import your preprocessors
    from neuronet.preprocessing.proxmox import ProxmoxDataProcessor
    from neuronet.preprocessing.scaphandre import ScaphandreProcessor

    proxmox_path = input_proxmox_dir.path
    scaphandre_path = input_scaphandre_dir.path

    # Step 1: Preprocess proxmox data
    proxmox_processor = ProxmoxDataProcessor(proxmox_path)
    proxmox_processor.run(output_csv="proxmox_processed.csv")
    proxmox_processed_path = os.path.join(proxmox_path, 'processed', 'proxmox_processed.csv')
    proxmox_df = pd.read_csv(proxmox_processed_path)
    print(f"Proxmox processed shape: {proxmox_df.shape}")

    # Step 2: Preprocess scaphandre data
    scaphandre_processor = ScaphandreProcessor(scaphandre_path)
    scaphandre_processor.run(output_csv="scaphandre_processed.csv")
    scaphandre_processed_path = os.path.join(scaphandre_path, 'processed', "vm_scaphandre_processed.csv")
    scaphandre_df = pd.read_csv(scaphandre_processed_path)
    print(f"Scaphandre processed shape: {proxmox_df.shape}")

    # Step 3: Combine datasets using EnergyDatasetBuilder
    builder = VmPowerDatasetBuilder(proxmox_df, scaphandre_df, interval='1min')
    print(scaphandre_df.columns.tolist())
    energy_dataset = builder.build()
    print(f"Combined energy dataset shape: {energy_dataset.shape}")

    # Step 4: Split into features and target
    X = energy_dataset[features]
    y = energy_dataset[target]

    # Step 5: Train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )

    # Step 6: Save splits as artifacts
    X_train.to_csv(output_x_train.path, index=False)
    X_test.to_csv(output_x_test.path, index=False)
    y_train.to_csv(output_y_train.path, index=False)
    y_test.to_csv(output_y_test.path, index=False)

    print("✅ Preprocessing done. Artifacts saved.")
