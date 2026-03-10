# Databricks Free ETL Pipeline Project

A portfolio-ready starter project for building a Databricks Free Edition stock-market ETL pipeline with a Medallion Architecture, Alpha Vantage ingestion, Delta Lake tables, and a monitoring UI that can run in Databricks Apps or Streamlit Cloud.

## Project Overview

This project is designed around the constraints of Databricks Free Edition. Instead of assuming always-on infrastructure, it uses a micro-batch polling model that still demonstrates streaming-style data engineering patterns:

- Bronze for raw Alpha Vantage payload ingestion
- Silver for schema enforcement, normalization, and deduplication
- Gold for business KPIs and dashboard-ready aggregates
- Streamlit for monitoring and data visualization
- Databricks Apps as the preferred in-platform UI host, with Streamlit Cloud as the fallback

## Why This Project Works On The Free Tier

Databricks Free Edition is suitable for learning and experimentation, but it is not a production environment. This starter takes that into account by:

- Treating Alpha Vantage as a frequently refreshed polling source instead of a true event stream
- Designing ingestion as short-lived, restartable micro-batches
- Persisting state in Delta so runs can resume after cluster shutdowns
- Keeping the watchlist intentionally small to stay within API limits
- Making the UI portable if Databricks Apps is unavailable in your workspace

## Repository Layout

```text
stock-dash-e2e/
├── app/
│   └── streamlit_app.py
├── config/
│   └── watchlist.yml
├── databricks/
│   └── notebooks/
│       ├── 01_bronze_ingestion.py
│       ├── 02_silver_transformations.py
│       ├── 03_gold_aggregations.py
│       └── 04_demo_microbatch_runner.py
├── docs/
│   ├── architecture.md
│   └── research.md
├── src/
│   └── stock_dash_etl/
│       ├── __init__.py
│       ├── alphavantage.py
│       ├── bronze.py
│       ├── config.py
│       ├── gold.py
│       ├── metrics.py
│       ├── schemas.py
│       └── silver.py
├── .env.example
├── .gitignore
├── LICENSE
├── README.md
└── requirements.txt
```

## Tech Stack

- Databricks Free Edition
- PySpark
- Delta Lake
- Requests for Alpha Vantage connectivity
- PyYAML for watchlist and settings
- Streamlit and Plotly for the monitoring UI
- Optional pandas export for local UI fallback data

## Medallion Architecture

### Bronze

The Bronze layer stores raw API responses and ingestion metadata. It is the replayable system of record for every pull from Alpha Vantage.

### Silver

The Silver layer flattens the nested Alpha Vantage payload, casts values to the expected types, standardizes timestamps, and removes duplicate symbol-timestamp combinations.

### Gold

The Gold layer computes dashboard-ready KPIs such as the latest close, price deltas, rolling averages, volume summaries, and ingestion freshness.

## Streaming Simulation Strategy

Because the free tier may not be appropriate for long-running jobs, the project uses a bounded micro-batch runner.

Recommended demo modes:

1. Manual notebook execution in Databricks
2. Bounded notebook loop for a short demo session
3. Periodic reruns triggered manually or by supported workflow features

## Databricks Apps And UI Strategy

Preferred deployment order:

1. Databricks Apps with Streamlit (queries Gold table directly via SQL warehouse)
2. Streamlit Cloud using exported Gold CSV data

The Streamlit app auto-detects its environment:

- **Databricks Apps**: reads from the Gold Delta table via `databricks-sql-connector` and your serverless SQL warehouse
- **Streamlit Cloud / local**: reads from the Gold CSV export at `UI_FALLBACK_GOLD_PATH`

### Deploying to Databricks Apps

1. In your Databricks workspace, create a new App and point it to `app/streamlit_app.py`
2. Set the following environment variables in the App configuration:
   - `DATABRICKS_HOST` — your workspace URL (e.g. `https://dbc-xxxxx.cloud.databricks.com`)
   - `DATABRICKS_SQL_WAREHOUSE_ID` — your serverless SQL warehouse ID
   - `DATABRICKS_TOKEN` — a personal access token (or leave empty if the App inherits credentials)
3. The app will query `stock_demo.gold_stock_kpis` directly — no CSV export needed

### Running on Streamlit Cloud

1. Export the Gold CSV by running notebook 3 (works when `/dbfs/` is writable, otherwise skip)
2. Or commit a local `data/demo_gold.csv` to the repo
3. Deploy to Streamlit Cloud pointing at `app/streamlit_app.py`

The UI focuses on:

- Latest ingestion status by symbol
- Row counts by layer
- Gold-layer KPI summaries
- Price trend and volume charts
- Freshness and pipeline health indicators

## Databricks Git Folder And Serverless Notes

If you open this repository from a Databricks Git folder, the Python entry points under `databricks/notebooks/` resolve the repo `src/` directory automatically, so you do not need to package-install the project just to run the ETL scripts.

The Streamlit app also resolves `src/` automatically when started from the repo.

Your serverless SQL warehouse is useful for:

- Querying Bronze, Silver, and Gold tables after the ETL runs
- Building SQL dashboards on top of Gold outputs
- Validating tables and KPIs without rerunning notebook code

The PySpark ETL itself still needs Databricks notebook compute with Spark support. A SQL warehouse alone is not the runtime for the Bronze, Silver, and Gold Python jobs.

## Alpha Vantage Notes

Alpha Vantage offers public stock market APIs, but the free tier is rate-limited to 25 API requests per day and 5 API requests per minute. This starter mitigates that by:

- Tracking a small watchlist
- Defaulting to the free `TIME_SERIES_DAILY` endpoint instead of premium intraday endpoints
- Using a safer default polling interval of 4 hours for a 3-symbol watchlist
- Spacing requests about 12 seconds apart inside each Bronze batch
- Failing fast when your configured watchlist and polling cadence would exceed the daily free-tier budget
- Supporting replay and idempotent writes
- Recommending demo-focused rather than always-on execution

With the default watchlist of `AAPL`, `MSFT`, and `NVDA`, a 4-hour polling cadence results in about 18 requests per day, leaving some headroom for manual testing.

The default configuration now uses daily market data. Repeated runs on the same trading day may produce the same source date until Alpha Vantage publishes the next daily bar. The Silver layer deduplicates by symbol and event timestamp.

The Alpha Vantage Postman collection is useful for manually exploring endpoints and testing request shapes during development:

- `https://www.postman.com/api-evangelist/blockchain/collection/j4n0jl2/alpha-vantage`

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
pip install -e .
```

### 2. Configure environment variables

Copy `.env.example` to `.env` and provide your Alpha Vantage key.

Recommended free-tier settings:

- `ALPHA_VANTAGE_FUNCTION=TIME_SERIES_DAILY`
- `DEFAULT_INTERVAL=daily`
- `POLL_SECONDS=14400`
- `ALPHA_VANTAGE_MAX_REQUESTS_PER_DAY=25`
- `ALPHA_VANTAGE_MAX_REQUESTS_PER_MINUTE=5`
- `DEMO_LOOP_SLEEP_SECONDS=15`

### 3. Review the watchlist

Update `config/watchlist.yml` with the symbols you want to track.

### 4. Run in Databricks

- Import the files under `databricks/notebooks/`
- Open the project from your Databricks Git folder
- Attach notebook compute with Spark support
- Set your API key using a secret or environment variable
- Execute Bronze, then Silver, then Gold notebooks
- Use your serverless SQL warehouse to query the resulting tables

### 5. Run the UI locally or in Streamlit Cloud

```bash
streamlit run app/streamlit_app.py
```

## Secrets Management

Recommended order of preference:

1. Databricks secret management if available in your workspace
2. Environment variables configured in the notebook session
3. Local `.env` for development only

Do not commit API keys to the repository.

## Portfolio Talking Points

- Adapting medallion data engineering patterns to free-tier platform limits
- Simulating streaming with restartable micro-batches
- Using Delta Lake for durability and resumability
- Handling API throttling and transient infrastructure in a portfolio-friendly way
- Separating platform-specific code from reusable transformation logic

## Next Steps

- Run the notebooks end to end in your Databricks workspace
- Validate whether Databricks Apps is enabled in your Free Edition environment
- Export Gold data for a Streamlit Cloud deployment if Apps is unavailable
- Add screenshots and a short architecture diagram for your GitHub portfolio
