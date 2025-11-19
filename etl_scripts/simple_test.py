# etl_scripts/final_test.py
from clickhouse_driver import Client

client = Client(host='localhost', user='admin', password='admin')
client.execute("CREATE DATABASE IF NOT EXISTS dwh")
client.execute("DROP TABLE IF EXISTS dwh.test")
client.execute("CREATE TABLE dwh.test (x UInt8) ENGINE = Memory")
client.execute("INSERT INTO dwh.test VALUES", [(1,), (2,)])
print("УСПЕХ:", client.execute("SELECT * FROM dwh.test"))