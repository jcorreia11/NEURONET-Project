import kfp
from kfp import dsl, kubernetes
from typing import List

from components.get_data import get_data
from components.preprocessing import preprocess_data
from components.optimize_and_train import optimize_model_hyperparameters_and_train
from components.evaluate import evaluate_model

@dsl.pipeline(
    name="Energy Usage Regression (Local PVC)",
    description="Fetch data, preprocess, train RF, evaluate using only local storage."
)
def energy_pipeline(
    features: List[str] = ["cpu_millicores", "memory_usage_mb", "logsfs_usage_percent"],
    target: str = "container_power_watts",
    test_size: float = 0.2,
    random_state: int = 42,
):

    pvc = kubernetes.CreatePVC(
        pvc_name_suffix='-energy-pvc',
        access_modes=['ReadWriteMany'],
        size='5Gi',
        storage_class_name='standard',
    )
    output_dir = "/mnt/shared"

    # 1. Get raw data
    data = get_data(
        output_dir=output_dir,
    )

    # 2. Preprocess
    pre = preprocess_data(
        data_dir=output_dir,
        features=features,
        target=target,
        test_size=test_size,
        random_state=random_state,
    ).after(data)

    # 3. Optimize hyperparameters and train best model
    optimize = optimize_model_hyperparameters_and_train(
        data_dir=output_dir,
        target=target,
        random_state=random_state,
    ).after(pre)


    # 4. Evaluate
    evaluate = evaluate_model(
        data_dir=output_dir,
        target=target,
    ).after(optimize)

    kubernetes.mount_pvc(
        data,
        pvc_name=pvc.outputs['name'],
        mount_path=output_dir
    )

    kubernetes.mount_pvc(
        pre,
        pvc_name=pvc.outputs['name'],
        mount_path=output_dir
    )

    kubernetes.mount_pvc(
        optimize,
        pvc_name=pvc.outputs['name'],
        mount_path=output_dir
    )

    kubernetes.mount_pvc(
        evaluate,
        pvc_name=pvc.outputs['name'],
        mount_path=output_dir
    )

    delete_pcv = kubernetes.DeletePVC(
        pvc_name=pvc.outputs['name']
    ).after(evaluate)


if __name__ == "__main__":
    import kfp.compiler as compiler
    compiler.Compiler().compile(
        pipeline_func=energy_pipeline,
        package_path="all-data-pipeline.yaml"
    )
    print("Pipeline compiled to all-data-pipeline.yaml")