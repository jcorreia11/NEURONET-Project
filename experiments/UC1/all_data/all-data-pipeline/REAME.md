# Pipeline 1 (energy-prediction)

# USECASE: energy-prediction    

## Uses: 
- FEATURES = ["cpuload", "mem_used", "swap_used"]

## To predict: 
- TARGET = "activePower"

## Inference:
- See: energy-prediction-inference.py

# Pipeline 2 (latency-prediction)

# USECASE: latency-prediction

## Uses:
- FEATURES = ["cpu_usage_nanocores", "memory_working_set_bytes",
              "memory_page_faults", "disk_used_percentage", "swap_used_percentage"]

## To predict:
- TARGET = "latency_ms"

## Inference:
- See: latency-prediction-inference.py

# Other info:

- Model API: http://fastapi-model-svc.admin.svc.cluster.local:8080

