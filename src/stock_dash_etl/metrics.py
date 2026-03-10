from __future__ import annotations

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
