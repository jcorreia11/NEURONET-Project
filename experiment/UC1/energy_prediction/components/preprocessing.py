from kfp.dsl import Output, Input, Dataset, component
from typing import List

@component(base_image="python:3.11",
           packages_to_install=[
               "git+https://github.com/jcorreia11/NEURONET-Project.git",
               "pandas==2.3.1",
               "scikit-learn==1.7.1"])
def preprocess_data(
    input_kepler_dir: Input[Dataset],
    input_k8s_dir: Input[Dataset],
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
    from neuronet.datasets.energy_dataset import EnergyDatasetBuilder

    # Import your preprocessors
    from neuronet.preprocessing.kepler import KeplerPreprocessor
    from neuronet.preprocessing.k8s import K8SProcessor

    kepler_path = input_kepler_dir.path
    k8s_path = input_k8s_dir.path

    # Step 1: Preprocess Kepler data
    kepler_processor = KeplerPreprocessor(kepler_path)
    kepler_processor.run(output_csv="kepler_processed.csv")
    kepler_processed_path = os.path.join(kepler_path, 'processed', 'kepler_processed.csv')
    kepler_df = pd.read_csv(kepler_processed_path)
    print(f"Kepler processed shape: {kepler_df.shape}")

    # Step 2: Preprocess K8S data
    k8s_processor = K8SProcessor(k8s_path)
    k8s_processor.run(output_csv="k8s_processed.csv")
    k8s_processed_path = os.path.join(k8s_path, 'processed', 'k8s_processed.csv')
    k8s_df = pd.read_csv(k8s_processed_path)
    print(f"K8S processed shape: {k8s_df.shape}")

    # Step 3: Combine datasets using EnergyDatasetBuilder
    builder = EnergyDatasetBuilder(k8s_df, kepler_df, interval='1min')
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
