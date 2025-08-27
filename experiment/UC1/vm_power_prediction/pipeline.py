import kfp
from kfp import dsl
from typing import List

from components.get_data import get_data
from components.preprocessing import preprocess_data
from components.training import train_model
from components.evaluate import evaluate_model

@dsl.pipeline(
    name="VM Power Prediction Regression (InfluxDB)",
    description="Fetch data from InfluxDB, preprocess, train MLP, evaluate."
)
def energy_pipeline(
    token: str,
    start: str = "2025-08-01T00:00:00Z",
    stop: str = "2025-08-05T23:59:59Z",
    features: List[str] = ['cpuload', 'mem_used_percentage', 'swap_used_percentage',
                           'disk_used_percentage', 'uptime_hours',
                           'scaph_process_cpu_usage_percentage',
                           'scaph_process_memory_bytes', 'scaph_process_memory_virtual_bytes',
                           'scaph_process_disk_total_read_bytes', 'scaph_process_disk_total_write_bytes'
                           ],
    target: str = "vm_power_watts",
    test_size: float = 0.2,
    random_state: int = 42,
    epochs: int = 20,
    lr: float = 0.001,
):
    # 1. Get raw data from InfluxDB (two datasets: kepler and k8s)
    data = get_data(
        token=token,
        start=start,
        stop=stop
    )

    # 2. Preprocess and merge datasets using EnergyDatasetBuilder
    pre = preprocess_data(
        input_proxmox_dir=data.outputs["output_proxmox_dir"],
        input_scaphandre_dir=data.outputs["output_scaphandre_dir"],
        features=features,
        target=target,
        test_size=test_size,
        random_state=random_state,
    )

    # 3. Train model using processed training data
    train = train_model(
        input_x_train=pre.outputs["output_x_train"],
        input_y_train=pre.outputs["output_y_train"],
        epochs=epochs,
        lr=lr,
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
        package_path="vm_energy_pipeline.yaml"
    )
    print("Pipeline compiled to vm_energy_pipeline.yaml")
