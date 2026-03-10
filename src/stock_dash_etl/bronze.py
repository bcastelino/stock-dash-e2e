from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

import requests
from pyspark.sql import SparkSession

from stock_dash_etl.alphavantage import fetch_intraday_payload, parse_last_refreshed
from stock_dash_etl.config import PipelineConfig
from stock_dash_etl.schemas import bronze_schema, bronze_state_schema


def build_bronze_records(config: PipelineConfig) -> list[dict[str, Any]]:
    config.validate_rate_limits()
    records: list[dict[str, Any]] = []
    session = requests.Session()
    try:
        for index, symbol in enumerate(config.symbols):
            if index > 0 and config.request_spacing_seconds > 0:
                time.sleep(config.request_spacing_seconds)
            request_ts = datetime.now(timezone.utc).replace(tzinfo=None)
            try:
                payload = fetch_intraday_payload(config, symbol, session=session)
                api_status = "ok"
                source_last_refreshed = parse_last_refreshed(payload)
            except Exception as exc:
                payload = {"error": str(exc), "symbol": symbol}
                api_status = "error"
                source_last_refreshed = None
            records.append(
                {
                    "symbol": symbol,
                    "function_name": config.function_name,
                    "interval": config.interval,
                    "request_ts": request_ts,
                    "source_last_refreshed": source_last_refreshed,
                    "ingested_at": datetime.now(timezone.utc).replace(tzinfo=None),
                    "api_status": api_status,
                    "raw_payload": json.dumps(payload),
                }
            )
    finally:
        session.close()
    return records


def write_bronze_batch(spark: SparkSession, config: PipelineConfig, records: list[dict[str, Any]]) -> None:
    if not records:
        return
    _ensure_schema(spark, config)
    frame = spark.createDataFrame(records, schema=bronze_schema)
    frame.write.format("delta").mode("append").saveAsTable(config.bronze_table_name)


def write_ingestion_state(spark: SparkSession, config: PipelineConfig, records: list[dict[str, Any]]) -> None:
    if not records:
        return
    _ensure_schema(spark, config)
    state_records = [
        {
            "symbol": record["symbol"],
            "last_request_ts": record["request_ts"],
            "last_status": record["api_status"],
            "updated_at": datetime.now(timezone.utc).replace(tzinfo=None),
        }
        for record in records
    ]
    state_frame = spark.createDataFrame(state_records, schema=bronze_state_schema)
    state_frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        config.bronze_state_table_name
    )


def _ensure_schema(spark: SparkSession, config: PipelineConfig) -> None:
    if config.catalog_name:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.catalog_name}.{config.schema_name}")
        return
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.schema_name}")
