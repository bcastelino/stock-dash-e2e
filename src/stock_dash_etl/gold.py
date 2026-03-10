from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from stock_dash_etl.config import PipelineConfig


def build_gold_frame(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    silver = spark.table(config.silver_table_name)

    latest_window = Window.partitionBy("symbol").orderBy(F.col("event_ts").desc())
    ordered_window = Window.partitionBy("symbol").orderBy(F.col("event_ts"))

    enriched = (
        silver.withColumn("previous_close_price", F.lag("close_price").over(ordered_window))
        .withColumn("latest_rank", F.row_number().over(latest_window))
        .withColumn("avg_close_price", F.avg("close_price").over(Window.partitionBy("symbol")))
        .withColumn("max_volume", F.max("volume").over(Window.partitionBy("symbol")))
        .withColumn("row_count", F.count(F.lit(1)).over(Window.partitionBy("symbol")))
    )

    gold = enriched.filter(F.col("latest_rank") == 1).select(
        F.col("symbol"),
        F.col("event_ts").alias("latest_event_ts"),
        F.col("open_price").alias("latest_open_price"),
        F.col("high_price").alias("latest_high_price"),
        F.col("low_price").alias("latest_low_price"),
        F.col("close_price").alias("latest_close_price"),
        F.col("previous_close_price"),
        (F.col("close_price") - F.coalesce(F.col("previous_close_price"), F.col("close_price"))).alias("price_change"),
        F.when(
            F.col("previous_close_price").isNull() | (F.col("previous_close_price") == F.lit(0.0)),
            F.lit(0.0),
        )
        .otherwise(((F.col("close_price") - F.col("previous_close_price")) / F.col("previous_close_price")) * F.lit(100.0))
        .alias("price_change_pct"),
        F.col("avg_close_price"),
        F.col("max_volume"),
        (F.col("high_price") - F.col("low_price")).alias("intraday_range"),
        F.col("row_count"),
        F.col("ingested_at").alias("latest_ingested_at"),
        (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("ingested_at"))) / F.lit(60.0),
    ).toDF(
        "symbol",
        "latest_event_ts",
        "latest_open_price",
        "latest_high_price",
        "latest_low_price",
        "latest_close_price",
        "previous_close_price",
        "price_change",
        "price_change_pct",
        "avg_close_price",
        "max_volume",
        "intraday_range",
        "row_count",
        "latest_ingested_at",
        "minutes_since_ingestion",
    )
    return gold.orderBy(F.col("symbol"))


def write_gold_table(spark: SparkSession, config: PipelineConfig) -> None:
    gold = build_gold_frame(spark, config)
    gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(config.gold_table_name)


def export_gold_for_ui(spark: SparkSession, config: PipelineConfig) -> Path | None:
    gold = build_gold_frame(spark, config)
    output_path = config.ui_fallback_gold_file
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        gold.toPandas().to_csv(output_path, index=False)
        return output_path
    except OSError:
        return None
