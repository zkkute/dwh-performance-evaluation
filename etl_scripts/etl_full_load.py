# etl_scripts/etl_full_load.py
import os
import time
import pandas as pd
from clickhouse_driver import Client
from tqdm import tqdm
from config import DATA_DIR

def full_load(scale_gb: int, schema: str = "star"):
    client = Client(host='localhost', user='admin', password='admin')
    path = f"{DATA_DIR}/{scale_gb}GB"
    start_total = time.time()

    print(f"[FULL LOAD] {scale_gb} ГБ → схема {schema}")

    # Очистка
    client.execute("TRUNCATE TABLE IF EXISTS dwh.star_sales")
    if schema == "snowflake":
        client.execute("TRUNCATE TABLE IF EXISTS dwh.fact_sales")
        # + другие таблицы snowflake

    total_rows = 0
    chunk_size = 200_000 if scale_gb <= 10 else 500_000

    for chunk in tqdm(pd.read_csv(f"{path}/sales.csv", chunksize=chunk_size), desc="Загрузка sales"):
        # Денормализация для star
        cust = pd.read_csv(f"{path}/customers.csv")[["customer_id", "full_name", "city"]]
        prod = pd.read_csv(f"{path}/products.csv")[["product_id", "category"]]
        dates = pd.read_csv(f"{path}/dates.csv")[["date_id", "year", "month"]]

        chunk = chunk.merge(cust.add_prefix("customer_"), left_on="customer_id", right_on="customer_customer_id")
        chunk = chunk.drop(columns=["customer_customer_id"], errors='ignore')
        chunk = chunk.merge(prod.add_prefix("product_"), on="product_id")
        chunk = chunk.merge(dates, on="date_id")

        records = chunk.to_dict('records')
        client.execute("INSERT INTO dwh.star_sales VALUES", records)
        total_rows += len(chunk)

    duration = time.time() - start_total
    throughput_gbh = scale_gb * 3600 / duration

    result = {
        "scale_gb": scale_gb,
        "schema": schema,
        "load_type": "full",
        "duration_sec": round(duration, 1),
        "rows_loaded": total_rows,
        "throughput_gbh": round(throughput_gbh, 1)
    }
    print(f"Готово за {duration:.1f} сек | {throughput_gbh:.1f} ГБ/ч")
    return result

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--gb", type=int, required=True)
    args = parser.parse_args()
    full_load(args.gb)