# etl_scripts/run_queries.py
import time
import random
from clickhouse_driver import Client
from tqdm import tqdm

client = Client(host='localhost', user='admin', password='admin')

QUERIES = {
    "simple": [
        "SELECT count() FROM dwh.star_sales",
        "SELECT sum(amount) FROM dwh.star_sales WHERE year = 2023",
        "SELECT max(amount) FROM dwh.star_sales",
    ],
    "medium": [
        "SELECT product_category, sum(amount) FROM dwh.star_sales GROUP BY product_category",
        "SELECT year, month, sum(amount) FROM dwh.star_sales GROUP BY year, month ORDER BY year, month",
        "SELECT customer_city, avg(amount) FROM dwh.star_sales GROUP BY customer_city HAVING avg(amount) > 500",
    ],
    "complex": [
        "SELECT year, product_category, sum(amount) FROM dwh.star_sales GROUP BY year, product_category ORDER BY sum(amount) DESC LIMIT 10",
        "SELECT a.year, b.year, a.total - b.total FROM (SELECT year, sum(amount) as total FROM dwh.star_sales GROUP BY year) a JOIN (SELECT year+1 as year, sum(amount) as total FROM dwh.star_sales GROUP BY year) b ON a.year = b.year",
    ]
}

def run_benchmark(repeats: int = 10):
    results = []
    for qtype, queries in QUERIES.items():
        for q in queries:
            for _ in tqdm(range(repeats), desc=f"{qtype} queries"):
                start = time.time()
                client.execute(q)
                duration = time.time() - start
                results.append({"type": qtype, "duration_sec": duration})
    return results

if __name__ == "__main__":
    data = run_benchmark(repeats=20)
    import pandas as pd
    df = pd.DataFrame(data)
    print(df.groupby("type").duration_sec.describe())
    df.to_csv("../data/logs/query_benchmark.csv", index=False)