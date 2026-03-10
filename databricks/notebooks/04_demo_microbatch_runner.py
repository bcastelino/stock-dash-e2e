import sys
from pathlib import Path
from time import sleep

from pyspark.sql import SparkSession

def _resolve_repo_root() -> Path:
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        return (Path("/Workspace") / notebook_path.lstrip("/")).parents[2]
    except Exception:
        if "__file__" in globals():
            return Path(__file__).resolve().parents[2]
        return Path.cwd()


repo_root = _resolve_repo_root()
sys.path.insert(0, str(repo_root / "src"))

from stock_dash_etl.bronze import build_bronze_records, write_bronze_batch, write_ingestion_state
from stock_dash_etl.config import load_config
from stock_dash_etl.gold import export_gold_for_ui, write_gold_table
from stock_dash_etl.silver import write_silver_table

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
config = load_config()
config.validate_rate_limits()
iterations = 2

for current_iteration in range(iterations):
    records = build_bronze_records(config)
    write_bronze_batch(spark, config, records)
    write_ingestion_state(spark, config, records)
    write_silver_table(spark, config)
    write_gold_table(spark, config)
    output_path = export_gold_for_ui(spark, config)
    print(
        {
            "iteration": current_iteration + 1,
            "bronze_records_written": len(records),
            "silver_rows": spark.table(config.silver_table_name).count(),
            "gold_rows": spark.table(config.gold_table_name).count(),
            "ui_export": str(output_path),
        }
    )
    if current_iteration + 1 < iterations:
        sleep(config.demo_loop_sleep_seconds)
