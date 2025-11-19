# etl_scripts/create_tables.py
from clickhouse_driver import Client

client = Client(host='localhost', user='admin', password='admin')

def create_star():
    client.execute("CREATE DATABASE IF NOT EXISTS dwh")
    client.execute("DROP TABLE IF EXISTS dwh.star_sales SYNC")
    client.execute('''
    CREATE TABLE dwh.star_sales (
        sale_id UInt64,
        customer_id UInt32,
        product_id UInt32,
        date_id UInt32,
        quantity UInt16,
        amount Decimal(12,2),
        customer_name String,
        customer_city String,
        product_category String,
        year UInt16,
        month UInt8
    ) ENGINE = MergeTree()
    PARTITION BY year
    ORDER BY (date_id, customer_id, product_id)
    ''')
    print("Таблица star_sales создана")

if __name__ == "__main__":
    create_star()