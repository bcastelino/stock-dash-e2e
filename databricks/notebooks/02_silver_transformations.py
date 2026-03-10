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

from stock_dash_etl.config import load_config
from stock_dash_etl.silver import write_silver_table

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
config = load_config()
write_silver_table(spark, config)
count = spark.table(config.silver_table_name).count()
print({"silver_rows": count, "silver_table": config.silver_table_name})
