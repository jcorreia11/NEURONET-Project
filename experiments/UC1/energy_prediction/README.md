# Kubeflow Pipelines — Energy Regression (CSV)

This project converts your notebook workflow into a Kubeflow Pipelines (KFP) pipeline with **separate, reusable components**.

## Structure
```
kfp_energy_pipeline/
├── components/
│   ├── get_data.py
│   ├── preprocessing.py
│   ├── training.py
│   └── evaluate.py
└── pipeline_energy.py
```

## What it does
- **get_data**: copies the input CSV into a KFP artifact.
- **preprocess_data**: selects features/target and splits into train/test.
- **train_model**: trains a `RandomForestRegressor`.
- **evaluate_model**: computes MAE, MSE, and R² and logs them to KFP UI.

## Usage
1. Ensure your cluster/image has access to the CSV file path you pass to the pipeline (PVC, mounted volume, or remote download you perform beforehand).
2. Compile the pipeline spec:
   ```bash
   python pipeline.py
   ```
   This produces `energy_pipeline.yaml`.
3. Upload `energy_pipeline.yaml` to Kubeflow Pipelines, set `csv_path` to your dataset (e.g., `/data/energy_dataset.csv`), and run.

## Parameters
- `csv_path`: path to the CSV with columns `cpu_millicores`, `memory_usage_mb`, `logsfs_usage_percent`, and `container_power_watts`.
- `features`: list of feature column names (defaults match the notebook).
- `target`: target column name.
- `test_size`, `random_state`: train/test split params.
- `n_estimators`: RandomForest number of trees.

## Notes
- Components specify lightweight base images and `packages_to_install` for reproducibility.
- If you prefer to fetch from InfluxDB (like your other pipeline), replace `get_data.py` with an InfluxDB reader that writes a CSV artifact and keep the rest unchanged.
