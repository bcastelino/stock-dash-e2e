from __future__ import annotations

from datetime import datetime
from typing import Any

import requests

from stock_dash_etl.config import PipelineConfig


def fetch_intraday_payload(
    config: PipelineConfig,
    symbol: str,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    if not config.api_key:
        raise ValueError("ALPHA_VANTAGE_API_KEY is required.")

    client = session or requests.Session()
    response = client.get(
        config.base_url,
        params={
            "function": config.function_name,
            "symbol": symbol,
            "interval": config.interval,
            "outputsize": config.outputsize,
            "apikey": config.api_key,
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()

    if "Error Message" in payload:
        raise RuntimeError(str(payload["Error Message"]))
    if "Note" in payload:
        raise RuntimeError(str(payload["Note"]))
    if resolve_time_series_key(payload) is None:
        raise RuntimeError("Alpha Vantage response did not include a time series payload.")
    return payload


def resolve_time_series_key(payload: dict[str, Any]) -> str | None:
    for key in payload.keys():
        if key.startswith("Time Series"):
            return key
    return None


def parse_last_refreshed(payload: dict[str, Any]) -> datetime | None:
    metadata = payload.get("Meta Data", {}) or {}
    raw_value = metadata.get("3. Last Refreshed") or metadata.get("4. Last Refreshed")
    if not raw_value:
        return None
    return parse_alpha_vantage_timestamp(str(raw_value))


def parse_alpha_vantage_timestamp(raw_value: str) -> datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw_value, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unsupported Alpha Vantage timestamp: {raw_value}")
