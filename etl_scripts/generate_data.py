# etl_scripts/generate_data.py — ФИНАЛЬНАЯ ВЕРСИЯ
import os
import pandas as pd
import numpy as np
from faker import Faker
import argparse

fake = Faker()

def generate(scale_gb: int):
    out_dir = f"data/generated/{scale_gb}GB"
    os.makedirs(out_dir, exist_ok=True)

    n_sales = {1: 5_000_000, 10: 50_000_000, 100: 500_000_000}[scale_gb]
    print(f"Генерация {scale_gb} ГБ ({n_sales:,} строк)...")

    # Измерения
    customers = pd.DataFrame({
        "customer_id": range(1, n_sales//1000 + 1),
        "customer_name": [fake.name() for _ in range(n_sales//1000)],
        "customer_city": [fake.city() for _ in range(n_sales//1000)]
    })

    products = pd.DataFrame({
        "product_id": range(1, 10001),
        "product_category": np.random.choice(["Electronics", "Clothing", "Food", "Books"], 10000)
    })

    dates = pd.date_range("2018-01-01", "2025-12-31").to_frame(name="date")
    dates["date_id"] = range(1, len(dates)+1)
    dates["year"] = dates["date"].dt.year
    dates["month"] = dates["date"].dt.month

    # Фактовая таблица
    sales = pd.DataFrame({
        "sale_id": range(1, n_sales + 1),
        "customer_id": np.random.randint(1, len(customers)+1, n_sales),
        "product_id": np.random.randint(1, len(products)+1, n_sales),
        "date_id": np.random.choice(dates["date_id"], n_sales),
        "quantity": np.random.randint(1, 10, n_sales),
        "amount": np.round(np.random.uniform(10, 1000, n_sales), 2)
    })

    customers.to_csv(f"{out_dir}/customers.csv", index=False)
    products.to_csv(f"{out_dir}/products.csv", index=False)
    dates[["date_id", "year", "month"]].to_csv(f"{out_dir}/dates.csv", index=False)
    sales.to_csv(f"{out_dir}/sales.csv", index=False)
    print(f"Готово → {out_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--gb", type=int, required=True)
    args = parser.parse_args()
    generate(args.gb)