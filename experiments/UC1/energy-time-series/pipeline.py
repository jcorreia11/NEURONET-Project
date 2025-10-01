import kfp
from kfp import dsl
from typing import List

from components.get_data import get_data
from components.preprocessing import preprocess_data
from components.train_evaluate import train_evaluate_model

@dsl.pipeline(
    name="Energy Usage Regression (InfluxDB)",
    description="Fetch data from InfluxDB, preprocess, train RF, evaluate."
)
def energy_pipeline(
    token: str,
    start: str = "2025-08-23T19:10:50Z",
    stop: str = "2025-08-25T19:10:57Z",
    features: List[str] = ["cpu_millicores", "memory_usage_mb", "logsfs_usage_percent", "container_power_watts"],
    test_perc: float = 0.2,
    val_perc: float = 0.1,
    horizon: int = 10,  # predict next 10 minutes (assuming 1-min frequency)
    lookback: int = 60  # use last 60 minutes to predict
):
    # 1. Get raw data from InfluxDB (two datasets: kepler and k8s)
    data = get_data(
        token=token,
        start=start,
        stop=stop
    )

    # 2. Preprocess and merge datasets using EnergyDatasetBuilder
    pre = preprocess_data(
        input_kepler_dir=data.outputs["output_kepler_dir"],
        input_k8s_dir=data.outputs["output_k8s_dir"],
        features=features,
        test_perc=test_perc,
        val_perc=val_perc,
    )

    # 3. Train and evaluate model using processed training data
    train = train_evaluate_model(
        input_train=pre.outputs["output_train"],
        input_val=pre.outputs["output_val"],
        input_test=pre.outputs["output_test"],
        input_scaler=pre.outputs["output_scaler"],
        lookback=lookback,
        horizon=horizon
    )

if __name__ == "__main__":
    import kfp.compiler as compiler
    compiler.Compiler().compile(
        pipeline_func=energy_pipeline,
        package_path="time-series-energy-pipeline.yaml"
    )
    print("Pipeline compiled to time-series-energy-pipeline.yaml")
