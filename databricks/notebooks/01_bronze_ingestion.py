import sys
from pathlib import Path

from pyspark.sql import SparkSession

repo_root = Path(__file__).resolve().parents[2] if "__file__" in globals() else Path.cwd()
sys.path.insert(0, str(repo_root / "src"))

from stock_dash_etl.bronze import build_bronze_records, write_bronze_batch, write_ingestion_state
from stock_dash_etl.config import load_config

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
config = load_config()
records = build_bronze_records(config)
write_bronze_batch(spark, config, records)
write_ingestion_state(spark, config, records)
print({"bronze_records_written": len(records), "bronze_table": config.bronze_table_name})
