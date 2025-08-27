from kfp.dsl import Output, Dataset, component

@component(
    base_image="python:3.11",
    packages_to_install=[
        "git+https://github.com/jcorreia11/NEURONET-Project.git",
        "influxdb-client==1.49.0",
        "pandas==2.3.1",
        "click==8.2.1",
        "python-dotenv==1.1.1",
        "scikit-learn==1.7.1",
        "joblib==1.4.2"
    ]
)
def get_data(token: str, start: str, stop: str,
             output_proxmox_dir: Output[Dataset],
             output_scaphandre_dir: Output[Dataset]):
    def run_query_and_save(token, start, stop, plugin, output_dir):
        import os
        import glob
        import shutil
        import subprocess

        tmp_dir = f"/tmp/{plugin}_raw"
        os.makedirs(tmp_dir, exist_ok=True)

        # Run influx query, saving all CSVs in tmp_dir
        subprocess.run([
            "query-influxdb",
            "--token", token,
            "--range", f"start: {start}, stop: {stop}",
            "--plugin", plugin,
            "--output-dir", tmp_dir
        ], check=True)

        # Copy all CSVs to the Kubeflow artifact directory
        os.makedirs(output_dir.path, exist_ok=True)
        for csv_file in glob.glob(os.path.join(tmp_dir, "*.csv")):
            shutil.copy(csv_file, output_dir.path)
            print(f"{plugin} CSV copied: {csv_file} -> {output_dir.path}")

    run_query_and_save(token, start, stop, "proxmox", output_proxmox_dir)
    run_query_and_save(token, start, stop, "scaphandre", output_scaphandre_dir)

    print("✅ Data fetching done. CSVs saved in plugin directories.")