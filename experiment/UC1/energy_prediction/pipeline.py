import kfp
from kfp import dsl
from typing import List

from components.get_data import get_data
from components.preprocessing import preprocess_data
from components.training import train_model
from components.evaluate import evaluate_model

@dsl.pipeline(
    name="Energy Usage Regression (InfluxDB)",
    description="Fetch data from InfluxDB, preprocess, train RF, evaluate."
)
def energy_pipeline(
    token: str,
    start: str = "2025-08-01T00:00:00Z",
    stop: str = "2025-08-05T23:59:59Z",
    features: List[str] = ["cpu_millicores", "memory_usage_mb", "logsfs_usage_percent"],
    target: str = "container_power_watts",
    test_size: float = 0.2,
    random_state: int = 42,
    n_estimators: int = 100,
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
        target=target,
        test_size=test_size,
        random_state=random_state,
    )

    # 3. Train model using processed training data
    train = train_model(
        input_x_train=pre.outputs["output_x_train"],
        input_y_train=pre.outputs["output_y_train"],
        n_estimators=n_estimators,
        random_state=random_state,
    )

    # 4. Evaluate model on test set
    evaluate = evaluate_model(
        input_x_test=pre.outputs["output_x_test"],
        input_y_test=pre.outputs["output_y_test"],
        input_model=train.outputs["output_model"],
    )

if __name__ == "__main__":
    import kfp.compiler as compiler
    compiler.Compiler().compile(
        pipeline_func=energy_pipeline,
        package_path="energy_pipeline.yaml"
    )
    print("Pipeline compiled to energy_pipeline.yaml")
