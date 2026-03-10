import os
import sys
from pathlib import Path

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

secret_scope = os.getenv("DATABRICKS_SECRET_SCOPE", "stock-dash-e2e")
secret_key = os.getenv("ALPHA_VANTAGE_API_KEY_SECRET_KEY", "alpha-vantage-api-key")
if not os.getenv("ALPHA_VANTAGE_API_KEY"):
    try:
        os.environ["ALPHA_VANTAGE_API_KEY"] = dbutils.secrets.get(scope=secret_scope, key=secret_key)
    except Exception:
        pass

from stock_dash_etl.config import load_config
from stock_dash_etl.gold import export_gold_for_ui, write_gold_table

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
config = load_config()
write_gold_table(spark, config)
output_path = export_gold_for_ui(spark, config)
count = spark.table(config.gold_table_name).count()
print({"gold_rows": count, "gold_table": config.gold_table_name, "ui_export": str(output_path)})
