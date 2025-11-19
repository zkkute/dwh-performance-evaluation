# etl_scripts/mvp_dwh_evaluator.py
import os
import time
import pandas as pd
from datetime import datetime
from clickhouse_driver import Client
from tqdm import tqdm
from config import DATA_DIR, LOG_DIR

SCALE_GB = 1  # ← МЕНЯЙ: 1, 10, 100
client = Client(host='localhost', user='admin', password='admin')


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}")


def main():
    log(f"ЗАПУСК ЭКСПЕРИМЕНТА: {SCALE_GB} ГБ, схема STAR")

    # 1. Генерация
    if not os.path.exists(f"{DATA_DIR}/{SCALE_GB}GB/sales.csv"):
        log("Генерируем данные...")
        os.system(f"python etl_scripts/generate_data.py --gb {SCALE_GB}")

    # 2. Создание таблиц
    os.system("python etl_scripts/create_tables.py")

    # 3. Full Load + замер времени
    log("Full Load начало")
    start = time.time()
    path = f"{DATA_DIR}/{SCALE_GB}GB"

    # Загружаем только sales — она самая большая
    chunk_size = 100_000 if SCALE_GB <= 10 else 500_000
    total_rows = 0
    for chunk in pd.read_csv(f"{path}/sales.csv", chunksize=chunk_size):
        # Денормализуем на лету для star-схемы
        cust = pd.read_csv(f"{path}/customers.csv")[["customer_id", "full_name", "city"]]
        prod = pd.read_csv(f"{path}/products.csv")[["product_id", "category"]]
        dates = pd.read_csv(f"{path}/dates.csv")[["date_id", "year", "month"]]

        chunk = chunk.merge(cust.add_prefix("customer_"), left_on="customer_id", right_on="customer_customer_id").drop(
            columns=["customer_customer_id"])
        chunk = chunk.merge(prod.add_prefix("product_"), on="product_id")
        chunk = chunk.merge(dates, on="date_id")

        client.execute(
            "INSERT INTO dwh.star_sales VALUES",
            chunk.to_dict('records')
        )
        total_rows += len(chunk)

    duration = time.time() - start
    log(f"Full Load завершён: {duration:.1f} сек, {total_rows / 1e6:.2f} млн строк")
    log(f"Пропускная способность: {SCALE_GB * 1000 / duration:.1f} ГБ/ч")

    # 4. Простые запросы
    log("Выполняем тестовые запросы...")
    queries = [
        "SELECT count() FROM dwh.star_sales",
        "SELECT sum(amount) FROM dwh.star_sales WHERE year = 2023",
        "SELECT product_category, sum(amount) FROM dwh.star_sales GROUP BY product_category"
    ]
    times = []
    for q in queries * 10:
        st = time.time()
        client.execute(q)
        times.append(time.time() - st)
    avg = sum(times) / len(times)
    log(f"Среднее время запроса: {avg:.3f} сек")

    log("ЭКСПЕРИМЕНТ УСПЕШНО ЗАВЕРШЁН! Можно запускать 10 ГБ")


if __name__ == "__main__":
    main()