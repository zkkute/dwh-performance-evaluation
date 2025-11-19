# etl_scripts/analyze_metrics.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Пример расчёта интегральной метрики E
def calculate_E():
    data = [
        {"schema": "star", "etl_time": 120, "query_time": 0.5, "throughput": 300},
        {"schema": "snowflake", "etl_time": 180, "query_time": 1.2, "throughput": 200},
        {"schema": "normalized", "etl_time": 250, "query_time": 1.8, "throughput": 150},
    ]
    df = pd.DataFrame(data)

    # Нормализация и веса
    df["E"] = 0.4 / df["etl_time"] + 0.3 / df["query_time"] + 0.3 * df["throughput"]
    df["E"] = df["E"] / df["E"].max()  # нормализация к 1

    print(df[["schema", "E"]])

    plt.figure(figsize=(8,5))
    sns.barplot(x="schema", y="E", data=df)
    plt.title("Интегральная метрика эффективности E")
    plt.ylabel("E (чем выше — тем лучше)")
    plt.savefig("../data/logs/E_metric.png")
    plt.show()

if __name__ == "__main__":
    calculate_E()