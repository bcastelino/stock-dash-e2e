import sys
from pathlib import Path

from pyspark.sql import SparkSession

repo_root = Path(__file__).resolve().parents[2] if "__file__" in globals() else Path.cwd()
sys.path.insert(0, str(repo_root / "src"))

from stock_dash_etl.config import load_config
from stock_dash_etl.silver import write_silver_table

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
config = load_config()
write_silver_table(spark, config)
count = spark.table(config.silver_table_name).count()
print({"silver_rows": count, "silver_table": config.silver_table_name})
