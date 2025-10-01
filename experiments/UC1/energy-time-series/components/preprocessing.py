from kfp.dsl import Output, Input, Dataset, Model, component
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
    test_perc: float,
    val_perc: float,
    output_train: Output[Dataset],
    output_val: Output[Dataset],
    output_test: Output[Dataset],
    output_scaler: Output[Model],
):
    import pandas as pd
    import os
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
    energy_dataset['_time'] = pd.to_datetime(energy_dataset['_time'])
    energy_dataset = energy_dataset.set_index('_time').sort_index().reset_index()
    print(f"Combined energy dataset shape: {energy_dataset.shape}")

    # Step 4: Split into features and target
    df = energy_dataset[features]

    # Step 5: Timeseries splits
    train_end = int(len(df) * (1 - test_perc - val_perc))
    val_end = int(len(df) * (1 - test_perc))
    train_df = df.iloc[:train_end]
    val_df = df.iloc[train_end:val_end]
    test_df = df.iloc[val_end:]

    # Step 6: Scale features
    from sklearn.preprocessing import StandardScaler

    scaler = StandardScaler()
    scaler.fit(train_df)

    # save scaler
    import joblib
    joblib.dump(scaler, output_scaler.path)

    train_scaled = pd.DataFrame(
        scaler.transform(train_df),
        columns=train_df.columns,
        index=train_df.index
    )
    val_scaled = pd.DataFrame(
        scaler.transform(val_df),
        columns=val_df.columns,
        index=val_df.index
    )
    test_scaled = pd.DataFrame(
        scaler.transform(test_df),
        columns=test_df.columns,
        index=test_df.index
    )

    # Save the splits
    train_scaled.to_csv(output_train.path, index=False)
    val_scaled.to_csv(output_val.path, index=False)
    test_scaled.to_csv(output_test.path, index=False)

    print("✅ Preprocessing done. Artifacts saved.")
