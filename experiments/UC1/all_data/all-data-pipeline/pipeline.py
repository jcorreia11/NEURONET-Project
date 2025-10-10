import kfp
from kfp import dsl
from typing import List

from components.get_data import get_data
from components.preprocessing import preprocess_data
from components.optimize_and_train import optimize_model_hyperparameters_and_train
from components.evaluate import evaluate_model

@dsl.pipeline(
    name="Energy Usage Regression (MinIO)",
    description="Fetch data, preprocess, train RF, evaluate using MinIO storage."
)
def energy_pipeline(
    features: List[str] = ["cpuload", "mem_used", "swap_used"],
    target: str = "activePower",
    test_size: float = 0.2,
    random_state: int = 42,
    usecase: str = "energy_prediction",
    post_base_url: str = "http://fastapi-model-svc.admin.svc.cluster.local:8080",
):
    # 1. Get raw data
    data_task = get_data()

    # 2. Preprocess
    preprocess_task = preprocess_data(
        df=data_task.outputs['data_csv'],
        features=features,
        target=target,
        test_size=test_size,
        random_state=random_state,
    )

    # 3. Optimize hyperparameters and train best model
    train_model_task = optimize_model_hyperparameters_and_train(
        train_data=preprocess_task.outputs['train_data'],
        target=target,
        random_state=random_state,
        usecase=usecase,
        post_base_url=post_base_url,
    )

    # 4. Evaluate
    evaluate_model(
        test_data=preprocess_task.outputs['test_data'],
        model=train_model_task.outputs['model_file'],
    )


if __name__ == "__main__":
    import kfp.compiler as compiler
    compiler.Compiler().compile(
        pipeline_func=energy_pipeline,
        package_path="all-data-pipeline.yaml"
    )
    print("Pipeline compiled to all-data-pipeline.yaml")
