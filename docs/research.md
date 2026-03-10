# Research Notes

## Databricks Free Edition

Databricks Free Edition replaces the older Community Edition positioning for learning and experimentation. Public Databricks materials describe it as a free environment for notebooks, SQL, data engineering exploration, and AI learning workflows.

### Verified Direction From Public Docs

- Databricks has a current Free Edition sign-up and limitations section.
- Databricks maintains first-party documentation for Structured Streaming.
- Databricks maintains first-party documentation for Delta Lake.
- Databricks maintains first-party documentation for Databricks Apps.
- Free Edition has documented limitations, so capabilities must be validated against the specific workspace at demo time.

### Practical Free-Tier Assumptions For This Project

- Short-lived interactive compute is more realistic than always-on jobs.
- Long-running streaming workloads may be impractical for portfolio demos.
- Scheduling, administration, and some platform features may be restricted.
- The project should be restartable and tolerant of cluster resets.

## Structured Streaming

Structured Streaming remains the canonical Databricks approach for near-real-time incremental processing. For this project, the platform concept is useful even if a true persistent streaming job is not the best fit for Free Edition.

### Streaming Implications For This Repo

The implementation uses bounded micro-batches that preserve streaming ideas:

- incremental ingestion
- checkpoint-like persisted state
- idempotent writes
- freshness monitoring

## Delta Lake

Databricks positions Delta Lake as the storage layer powering reliable lakehouse tables. The most relevant capabilities for this project are:

- reliable table storage for Bronze, Silver, and Gold
- schema-aware ingestion and downstream transformations
- support for incremental workloads
- strong fit for restartable pipelines after compute interruptions

## Databricks Apps

Databricks provides a Databricks Apps framework with official documentation. Workspace-level availability can vary, so this project treats Apps as the preferred in-platform host but not a guaranteed dependency.

### UI Decision

- Preferred: Streamlit in Databricks Apps
- Secondary: Dash in Databricks Apps
- Fallback: Streamlit Cloud with exported Gold data or direct API/database connectivity if supported

## Alpha Vantage

Alpha Vantage provides public stock market APIs suitable for a portfolio project.

### Why Alpha Vantage Fits

- simple REST interface
- stock intraday and daily time series endpoints
- easy onboarding for public demo projects
- enough frequency for polling-based micro-batches

### Constraints

Alpha Vantage publicly states that premium membership is required beyond the standard free usage limit of 25 API requests per day.

### Alpha Vantage Implications For This Repo

The implementation is intentionally conservative:

- small default watchlist
- longer polling cadence
- one request per symbol per run
- durable Delta outputs so repeated runs are still useful

## Risks To Validate In The Workspace

- whether Databricks Apps is visible in your Free Edition workspace
- whether secret management is available in the exact environment
- whether cluster runtime choices support the desired Delta and PySpark features
- whether notebook scheduling options are exposed or must be simulated manually
