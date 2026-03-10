# Architecture

## Objective

Build a free-tier-friendly Databricks ETL pipeline for stock data using a Medallion Architecture and a lightweight monitoring UI.

## Source

- Alpha Vantage REST API
- Endpoint choice: `TIME_SERIES_INTRADAY`
- Typical cadence: every 60 minutes for a small watchlist on the free plan

## Data Flow

```text
Alpha Vantage API
    -> Bronze raw ingestion table
    -> Silver normalized quote table
    -> Gold KPI table
    -> Streamlit dashboard
```

## Bronze Layer

### Bronze Responsibilities

- call Alpha Vantage for each tracked symbol
- store the raw JSON response
- attach metadata such as symbol, function, interval, request timestamp, and ingestion timestamp
- maintain a simple ingestion state table for restartability

### Storage Design

- Delta table name from environment configuration
- one row per symbol pull
- raw payload stored as JSON string

## Silver Layer

### Silver Responsibilities

- extract quote rows from the raw payload
- map source fields into typed columns
- standardize timestamps
- deduplicate on `symbol` plus `event_ts`
- preserve ingestion timestamps for freshness analysis

### Core Columns

- `symbol`
- `event_ts`
- `open_price`
- `high_price`
- `low_price`
- `close_price`
- `volume`
- `source_request_ts`
- `ingested_at`

## Gold Layer

### Gold Responsibilities

- publish latest price metrics by symbol
- compute price movement and intraday range metrics
- compute rolling averages for dashboard consumption
- compute freshness metrics for operations monitoring

### Example KPIs

- latest close price
- absolute price change
- percentage price change
- average close price
- maximum volume observed
- minutes since latest ingestion

## Streaming Pattern

This repo uses a polling-based micro-batch pattern rather than a true continuous stream.

### Why

- better aligned with free-tier compute realities
- compatible with API rate limits
- easier to demo and explain in a portfolio setting
- resilient to cluster termination and workspace resets

## UI Deployment Options

### Primary

- Databricks Apps with Streamlit

### Secondary

- Databricks Apps with Dash

### Fallback

- Streamlit Cloud

## Operational Design Choices

- small watchlist to reduce daily API consumption
- persisted state table to track the most recent pull per symbol
- Gold export path for UI fallback scenarios
- package-style transformation code so the same logic can run in notebooks and local tests
