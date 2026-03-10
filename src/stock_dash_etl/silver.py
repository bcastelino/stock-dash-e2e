from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from stock_dash_etl.config import PipelineConfig


def build_silver_frame(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    bronze_frame = spark.table(config.bronze_table_name).filter(F.col("api_status") == F.lit("ok"))
    time_series_path = f"$['Time Series ({config.interval})']"
    quote_map_schema = T.MapType(T.StringType(), T.MapType(T.StringType(), T.StringType()))

    payload_map = bronze_frame.withColumn("time_series_json", F.get_json_object(F.col("raw_payload"), time_series_path)).withColumn(
        "quote_map_by_ts",
        F.from_json(F.col("time_series_json"), quote_map_schema),
    )

    exploded = payload_map.select(
        "symbol",
        "request_ts",
        "source_last_refreshed",
        "ingested_at",
        F.explode_outer(F.col("quote_map_by_ts")).alias("event_ts_raw", "quote_map"),
    )

    silver = exploded.select(
        F.col("symbol"),
        F.to_timestamp("event_ts_raw").alias("event_ts"),
        F.col("quote_map")["1. open"].cast("double").alias("open_price"),
        F.col("quote_map")["2. high"].cast("double").alias("high_price"),
        F.col("quote_map")["3. low"].cast("double").alias("low_price"),
        F.col("quote_map")["4. close"].cast("double").alias("close_price"),
        F.col("quote_map")["5. volume"].cast("long").alias("volume"),
        F.col("request_ts").alias("source_request_ts"),
        F.col("source_last_refreshed"),
        F.col("ingested_at"),
    ).filter(F.col("event_ts").isNotNull())

    dedupe_window = Window.partitionBy("symbol", "event_ts").orderBy(F.col("ingested_at").desc())
    return silver.withColumn("row_num", F.row_number().over(dedupe_window)).filter(F.col("row_num") == 1).drop("row_num")


def write_silver_table(spark: SparkSession, config: PipelineConfig) -> None:
    silver = build_silver_frame(spark, config)
    silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(config.silver_table_name)
