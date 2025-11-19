# etl_scripts/collect_metrics.py
import time

import requests
import pandas as pd
from datetime import datetime, timedelta

def get_cpu_usage(minutes=10):
    end = int(time.time())
    start = end - minutes*60
    url = f"http://localhost:9090/api/v1/query_range"
    query = '100 - (avg by(instance)(irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)'
    params = {"query": query, "start": start, "end": end, "step": "30s"}
    r = requests.get(url, params=params)
    data = r.json()["data"]["result"][0]["values"]
    df = pd.DataFrame(data, columns=["ts", "value"])
    df["value"] = df["value"].astype(float)
    return df["value"].mean()

if __name__ == "__main__":
    print("Средняя загрузка CPU за последние 10 мин:", get_cpu_usage())