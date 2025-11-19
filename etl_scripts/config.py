# etl_scripts/config.py
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data", "generated")
LOG_DIR = os.path.join(BASE_DIR, "data", "logs")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 9000