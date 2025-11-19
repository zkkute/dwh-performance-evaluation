# etl_scripts/etl_incremental.py
import pandas as pd
import time
from clickhouse_driver import Client

def incremental_load(scale_gb: int):
    client = Client(host='localhost', user='admin', password='admin')
    path = f"../data/generated/{scale_gb}GB"

    print("[INCREMENTAL] Добавляем 10% новых данных...")

    start = time.time()
    sales = pd.read_csv(f"{path}/sales.csv").sample(frac=0.1, random_state=42)
    # простая денормализация как выше
    cust = pd.read_csv(f"{path}/customers.csv")[["customer_id", "full_name", "city"]]
    prod = pd.read_csv(f"{path}/products.csv")[["product_id", "category"]]
    dates = pd.read_csv(f"{path}/dates.csv")[["date_id", "year", "month"]]

    sales = sales.merge(cust.add_prefix("customer_"), left_on="customer_id", right_on="customer_customer_id").drop(columns=["customer_customer_id"], errors='ignore')
    sales = sales.merge(prod.add_prefix("product_"), on="product_id")
    sales = sales.merge(dates, on="date_id")

    # Добавляем колонку для MERGE (например, update_ts)
    sales["update_ts"] = time.time()

    client.execute("""
        ALTER TABLE dwh.star_sales ADD COLUMN IF NOT EXISTS update_ts Float64
    """)

    # Простой UPSERT через REPLACE (или MERGE в новых версиях)
    client.execute("INSERT INTO dwh.star_sales VALUES", sales.to_dict('records'))

    duration = time.time() - start
    print(f"Инкрементальная загрузка: {duration:.1f} сек")
    return {"load_type": "incremental", "duration_sec": duration, "rows": len(sales)}

if __name__ == "__main__":
    incremental_load(1)