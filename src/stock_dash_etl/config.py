from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path

import yaml
try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


@dataclass(frozen=True)
class PipelineConfig:
    api_key: str
    base_url: str
    symbols: list[str]
    function_name: str
    interval: str
    outputsize: str
    poll_seconds: int
    max_requests_per_day: int
    max_requests_per_minute: int
    demo_loop_sleep_seconds: int
    catalog_name: str
    schema_name: str
    bronze_table: str
    silver_table: str
    gold_table: str
    bronze_state_table: str
    ui_fallback_gold_path: str

    @property
    def bronze_table_name(self) -> str:
        return _qualify_table_name(self.catalog_name, self.schema_name, self.bronze_table)

    @property
    def silver_table_name(self) -> str:
        return _qualify_table_name(self.catalog_name, self.schema_name, self.silver_table)

    @property
    def gold_table_name(self) -> str:
        return _qualify_table_name(self.catalog_name, self.schema_name, self.gold_table)

    @property
    def bronze_state_table_name(self) -> str:
        return _qualify_table_name(self.catalog_name, self.schema_name, self.bronze_state_table)

    @property
    def time_series_key(self) -> str:
        if self.function_name == "TIME_SERIES_INTRADAY":
            return f"Time Series ({self.interval})"
        mapping = {
            "TIME_SERIES_DAILY": "Time Series (Daily)",
            "TIME_SERIES_DAILY_ADJUSTED": "Time Series (Daily)",
            "TIME_SERIES_WEEKLY": "Weekly Time Series",
            "TIME_SERIES_WEEKLY_ADJUSTED": "Weekly Adjusted Time Series",
            "TIME_SERIES_MONTHLY": "Monthly Time Series",
            "TIME_SERIES_MONTHLY_ADJUSTED": "Monthly Adjusted Time Series",
        }
        if self.function_name in mapping:
            return mapping[self.function_name]
        raise ValueError(f"Unsupported Alpha Vantage time series function: {self.function_name}")

    @property
    def ui_fallback_gold_file(self) -> Path:
        path = Path(self.ui_fallback_gold_path)
        if path.is_absolute():
            return path
        return _repo_root() / path

    @property
    def requests_per_run(self) -> int:
        return len(self.symbols)

    @property
    def request_spacing_seconds(self) -> int:
        if self.max_requests_per_minute <= 0:
            return 0
        return max(1, math.ceil(60 / self.max_requests_per_minute))

    @property
    def projected_daily_requests(self) -> int:
        if self.poll_seconds <= 0:
            return self.requests_per_run
        runs_per_day = math.ceil(86400 / self.poll_seconds)
        return runs_per_day * self.requests_per_run

    @property
    def recommended_poll_seconds(self) -> int:
        if self.requests_per_run == 0 or self.max_requests_per_day <= 0:
            return self.poll_seconds
        return math.ceil((86400 * self.requests_per_run) / self.max_requests_per_day)

    def validate_rate_limits(self) -> None:
        if self.projected_daily_requests > self.max_requests_per_day:
            raise ValueError(
                "Alpha Vantage free-tier budget exceeded by configuration. "
                f"Current setup would project about {self.projected_daily_requests} requests/day for "
                f"{self.requests_per_run} symbol(s). Set POLL_SECONDS to at least {self.recommended_poll_seconds} "
                f"seconds or reduce the watchlist."
            )


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _qualify_table_name(catalog_name: str, schema_name: str, table_name: str) -> str:
    if catalog_name:
        return f"{catalog_name}.{schema_name}.{table_name}"
    return f"{schema_name}.{table_name}"


def _split_symbols(raw_value: str) -> list[str]:
    return [item.strip().upper() for item in raw_value.split(",") if item.strip()]


def _read_watchlist(watchlist_path: Path) -> tuple[list[str], dict]:
    if not watchlist_path.exists():
        return [], {}
    with watchlist_path.open("r", encoding="utf-8") as handle:
        content = yaml.safe_load(handle) or {}
    symbols = [str(symbol).strip().upper() for symbol in content.get("symbols", []) if str(symbol).strip()]
    settings = content.get("settings", {}) or {}
    return symbols, settings


def _default_ui_fallback_gold_path() -> str:
    if os.getenv("DATABRICKS_RUNTIME_VERSION") or os.getenv("DB_HOME"):
        return "/dbfs/FileStore/stock-dash-e2e/demo_gold.csv"
    return "data/demo_gold.csv"


def load_config(env_file: str | None = None, watchlist_path: str | None = None) -> PipelineConfig:
    env_path = Path(env_file) if env_file else _repo_root() / ".env"
    if env_path.exists() and load_dotenv is not None:
        load_dotenv(env_path)

    resolved_watchlist = Path(watchlist_path) if watchlist_path else _repo_root() / "config" / "watchlist.yml"
    yaml_symbols, settings = _read_watchlist(resolved_watchlist)

    env_symbols = _split_symbols(os.getenv("WATCHLIST", ""))
    symbols = env_symbols or yaml_symbols or ["AAPL", "MSFT", "NVDA"]

    return PipelineConfig(
        api_key=os.getenv("ALPHA_VANTAGE_API_KEY", "").strip(),
        base_url=os.getenv("ALPHA_VANTAGE_BASE_URL", "https://www.alphavantage.co/query").strip(),
        symbols=symbols,
        function_name=os.getenv("ALPHA_VANTAGE_FUNCTION", str(settings.get("function", "TIME_SERIES_DAILY"))).strip(),
        interval=os.getenv("DEFAULT_INTERVAL", str(settings.get("interval", "daily"))).strip(),
        outputsize=os.getenv("ALPHA_VANTAGE_OUTPUTSIZE", str(settings.get("outputsize", "compact"))).strip(),
        poll_seconds=int(os.getenv("POLL_SECONDS", str(settings.get("poll_seconds", 14400)))),
        max_requests_per_day=int(
            os.getenv("ALPHA_VANTAGE_MAX_REQUESTS_PER_DAY", str(settings.get("max_requests_per_day", 25)))
        ),
        max_requests_per_minute=int(
            os.getenv("ALPHA_VANTAGE_MAX_REQUESTS_PER_MINUTE", str(settings.get("max_requests_per_minute", 5)))
        ),
        demo_loop_sleep_seconds=int(os.getenv("DEMO_LOOP_SLEEP_SECONDS", "15")),
        catalog_name=os.getenv("CATALOG_NAME", "").strip(),
        schema_name=os.getenv("SCHEMA_NAME", "stock_demo").strip(),
        bronze_table=os.getenv("BRONZE_TABLE", "bronze_stock_quotes").strip(),
        silver_table=os.getenv("SILVER_TABLE", "silver_stock_quotes").strip(),
        gold_table=os.getenv("GOLD_TABLE", "gold_stock_kpis").strip(),
        bronze_state_table=os.getenv("BRONZE_STATE_TABLE", "bronze_ingestion_state").strip(),
        ui_fallback_gold_path=os.getenv("UI_FALLBACK_GOLD_PATH", _default_ui_fallback_gold_path()).strip(),
    )
