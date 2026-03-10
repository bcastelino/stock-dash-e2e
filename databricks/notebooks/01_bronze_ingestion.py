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

supported_functions = {
    "TIME_SERIES_DAILY": "daily",
    "TIME_SERIES_WEEKLY": "weekly",
    "TIME_SERIES_MONTHLY": "monthly",
}
selected_function = os.getenv("ALPHA_VANTAGE_FUNCTION", "TIME_SERIES_DAILY")
if selected_function not in supported_functions:
    selected_function = "TIME_SERIES_DAILY"
try:
    dbutils.widgets.dropdown(
        "alpha_vantage_function",
        selected_function,
        list(supported_functions.keys()),
        "Alpha Vantage endpoint",
    )
    selected_function = dbutils.widgets.get("alpha_vantage_function")
except Exception:
    pass
os.environ["ALPHA_VANTAGE_FUNCTION"] = selected_function
os.environ["DEFAULT_INTERVAL"] = supported_functions[selected_function]

from stock_dash_etl.bronze import build_bronze_records, write_bronze_batch, write_ingestion_state
from stock_dash_etl.config import load_config

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
config = load_config()
records = build_bronze_records(config)
write_bronze_batch(spark, config, records)
write_ingestion_state(spark, config, records)
print({"bronze_records_written": len(records), "bronze_table": config.bronze_table_name})
