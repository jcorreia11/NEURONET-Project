import kfp
from kfp import dsl
from typing import List
from modules.components import full_pipeline


@dsl.pipeline(
    name="Energy Usage Regression End-to-End",
    description="Fetch data from InfluxDB, preprocess, train Random Forest, evaluate, and save artifacts."
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
    # Call the orchestrator component
    full_pipeline(
        token=token,
        start=start,
        stop=stop,
        features=features,
        target=target,
        test_size=test_size,
        n_estimators=n_estimators,
        random_state=random_state,
    )


if __name__ == "__main__":
    import kfp.compiler as compiler

    # Compile the pipeline to YAML
    compiler.Compiler().compile(
        pipeline_func=energy_pipeline,
        package_path="one_component_energy_pipeline.yaml"
    )
    print("Pipeline compiled to one_component_energy_pipeline.yaml")
