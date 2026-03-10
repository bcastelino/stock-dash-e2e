from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession, functions as F

from stock_dash_etl.config import PipelineConfig


def collect_table_counts(spark: SparkSession, config: PipelineConfig) -> dict[str, int]:
    return {
        "bronze": spark.table(config.bronze_table_name).count(),
        "silver": spark.table(config.silver_table_name).count(),
        "gold": spark.table(config.gold_table_name).count(),
    }


def is_databricks_apps() -> bool:
    return bool(os.getenv("DATABRICKS_HOST") and os.getenv("DATABRICKS_SQL_WAREHOUSE_ID"))


def read_gold_from_sql(gold_table: str) -> pd.DataFrame:
    try:
        from databricks import sql as dbsql
    except ImportError:
        return pd.DataFrame()

    host = os.getenv("DATABRICKS_HOST", "").strip().rstrip("/")
    warehouse_id = os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", "").strip()
    token = os.getenv("DATABRICKS_TOKEN", "").strip()

    if not host or not warehouse_id:
        return pd.DataFrame()

    http_path = f"/sql/1.0/warehouses/{warehouse_id}"
    conn_kwargs: dict = {"server_hostname": host.replace("https://", ""), "http_path": http_path}
    if token:
        conn_kwargs["access_token"] = token

    with dbsql.connect(**conn_kwargs) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {gold_table}")
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
    frame = pd.DataFrame(rows, columns=columns)
    for col in ["latest_event_ts", "latest_ingested_at"]:
        if col in frame.columns:
            frame[col] = pd.to_datetime(frame[col], errors="coerce")
    return frame


def read_gold_for_ui(csv_path: str | Path) -> pd.DataFrame:
    path = Path(csv_path)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, parse_dates=["latest_event_ts", "latest_ingested_at"])


def build_silver_history(spark: SparkSession, config: PipelineConfig):
    silver = spark.table(config.silver_table_name)
    return silver.select(
        "symbol",
        "event_ts",
        "close_price",
        "volume",
        F.to_date("event_ts").alias("event_date"),
    )
