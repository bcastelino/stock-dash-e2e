import sys
from pathlib import Path

from pyspark.sql import SparkSession

repo_root = Path(__file__).resolve().parents[2] if "__file__" in globals() else Path.cwd()
sys.path.insert(0, str(repo_root / "src"))

from stock_dash_etl.config import load_config
from stock_dash_etl.gold import export_gold_for_ui, write_gold_table

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
config = load_config()
write_gold_table(spark, config)
output_path = export_gold_for_ui(spark, config)
count = spark.table(config.gold_table_name).count()
print({"gold_rows": count, "gold_table": config.gold_table_name, "ui_export": str(output_path)})
