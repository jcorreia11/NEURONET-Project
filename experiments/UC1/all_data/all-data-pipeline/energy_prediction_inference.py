import pandas as pd
import joblib
import requests

FEATURES = ["cpuload", "mem_used", "swap_used"]
TARGET = "activePower"
BASE = "http://10.255.40.140:30080"
USECASE = "energy_prediction"

def single_input_inference(cpu_load, mem_used, swap_used):
    data = {
        "cpuload": [cpu_load],
        "mem_used": [mem_used],
        "swap_used": [swap_used],
    }
    scaler = joblib.load("scaler_file")
    input_df = pd.DataFrame(data)
    input_scaled = pd.DataFrame(scaler.transform(input_df), columns=FEATURES)
    data = {
        "x": [input_scaled.iloc[0]["cpuload"], input_scaled.iloc[0]["mem_used"], input_scaled.iloc[0]["swap_used"]]
    }


    prediction = requests.post(f"{BASE}/model_prediction/{USECASE}", json=data)

    prediction_value = prediction.json()['pred'][0]
    return {"predicted_activePower": prediction_value}

def batch_inference(input_csv_path):
    df = pd.read_csv(input_csv_path)

    # assure number of features match
    assert len(FEATURES) == df.shape[1], f"Expected {len(FEATURES)} features, got {df.shape[1]}"

    scaler = joblib.load("scaler_file")
    input_scaled = pd.DataFrame(scaler.transform(df[FEATURES]), columns=FEATURES)

    data = {
        "x": input_scaled.values.tolist()
    }

    prediction = requests.post(f"{BASE}/model_prediction/{USECASE}", json=data)

    prediction_values = prediction.json()['pred']
    df['predicted_activePower'] = prediction_values
    return df


if __name__ == "__main__":
    print(single_input_inference(20.0, 3000.0, 500.0))
    # create a sample input CSV
    sample_data = {
        "cpuload": [10.0, 20.0, 30.0],
        "mem_used": [2000.0, 3000.0, 4000.0],
        "swap_used": [100.0, 500.0, 1000.0],
    }
    sample_df = pd.DataFrame(sample_data)
    sample_input_path = "sample_input.csv"
    sample_df.to_csv(sample_input_path, index=False)
    batch_results = batch_inference(sample_input_path)
    print(batch_results)
    # delete file
    import os
    os.remove(sample_input_path)
