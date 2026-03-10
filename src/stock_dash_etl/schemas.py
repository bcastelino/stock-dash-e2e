from pyspark.sql import types as T

bronze_schema = T.StructType(
    [
        T.StructField("symbol", T.StringType(), False),
        T.StructField("function_name", T.StringType(), False),
        T.StructField("interval", T.StringType(), False),
        T.StructField("request_ts", T.TimestampType(), False),
        T.StructField("source_last_refreshed", T.TimestampType(), True),
        T.StructField("ingested_at", T.TimestampType(), False),
        T.StructField("api_status", T.StringType(), False),
        T.StructField("raw_payload", T.StringType(), False),
    ]
)

bronze_state_schema = T.StructType(
    [
        T.StructField("symbol", T.StringType(), False),
        T.StructField("last_request_ts", T.TimestampType(), False),
        T.StructField("last_status", T.StringType(), False),
        T.StructField("updated_at", T.TimestampType(), False),
    ]
)

silver_schema = T.StructType(
    [
        T.StructField("symbol", T.StringType(), False),
        T.StructField("event_ts", T.TimestampType(), False),
        T.StructField("open_price", T.DoubleType(), False),
        T.StructField("high_price", T.DoubleType(), False),
        T.StructField("low_price", T.DoubleType(), False),
        T.StructField("close_price", T.DoubleType(), False),
        T.StructField("volume", T.LongType(), False),
        T.StructField("source_request_ts", T.TimestampType(), False),
        T.StructField("source_last_refreshed", T.TimestampType(), True),
        T.StructField("ingested_at", T.TimestampType(), False),
    ]
)

gold_schema = T.StructType(
    [
        T.StructField("symbol", T.StringType(), False),
        T.StructField("latest_event_ts", T.TimestampType(), False),
        T.StructField("latest_open_price", T.DoubleType(), False),
        T.StructField("latest_high_price", T.DoubleType(), False),
        T.StructField("latest_low_price", T.DoubleType(), False),
        T.StructField("latest_close_price", T.DoubleType(), False),
        T.StructField("previous_close_price", T.DoubleType(), True),
        T.StructField("price_change", T.DoubleType(), False),
        T.StructField("price_change_pct", T.DoubleType(), False),
        T.StructField("avg_close_price", T.DoubleType(), False),
        T.StructField("max_volume", T.LongType(), False),
        T.StructField("intraday_range", T.DoubleType(), False),
        T.StructField("row_count", T.LongType(), False),
        T.StructField("latest_ingested_at", T.TimestampType(), False),
        T.StructField("minutes_since_ingestion", T.DoubleType(), False),
    ]
)
