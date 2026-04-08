# Abuja Weather Intelligence Pipeline

> **Business problem:** Logistics operators, agricultural planners, and field operations teams in Nigeria make daily decisions affected by weather — but there is no centralized, queryable, historically-accumulating weather store for Nigerian cities. This pipeline creates one, and surfaces a daily operational risk signal on top of it.

---

## What This Project Actually Does

Every hour, this pipeline:

1. Fetches hourly forecast data for Abuja from the Open-Meteo API
2. Streams it into a raw BigQuery table — untouched, exactly as received
3. Runs dbt transformations that clean, deduplicate, and roll up the data into a mart table
4. Produces a `logistics_risk_level` signal per calendar day (`Low`, `Moderate`, or `High Risk`) based on precipitation thresholds — a concrete, business-interpretable output, not just raw numbers

The mart table is designed to answer questions like:

- *How many high-risk weather days has Abuja had this month?*
- *Which days last week were safe for outdoor field operations?*
- *What is the seasonal precipitation pattern across the year?*

---

## Architecture

```mermaid
flowchart LR
    subgraph ingest [Ingestion]
        API[Open-Meteo API]
        PY[ingest.py]
        API --> PY
    end

    subgraph gcp [Google Cloud / BigQuery]
        RAW[(weather_data.abuja_hourly\nRaw — append-only, never modified)]
        PY -->|insert_rows_json| RAW
    end

    subgraph dbt [dbt Transformation Layer]
        STG[stg_weatherdata_raw\nStaging view\nTyped · Deduplicated · Null-filtered]
        MART[mart_daily_forecast\nMart table\nDaily aggregates · Risk bands]
        RAW --> STG
        STG --> MART
    end
```

### Why this architecture?

| Decision | Rationale |
|---|---|
| **Raw table is append-only** | Preserves source truth. If transformation logic changes, the raw data can be reprocessed without re-fetching from the API. This is a foundational production pattern. |
| **Staging as a view, mart as a table** | Staging is cheap to recompute and doesn't need to be stored. The mart is queried by downstream consumers and benefits from materialisation. |
| **BigQuery over Postgres** | This is an analytical workload — columnar storage and serverless scaling make BigQuery the right fit. Postgres would work at this volume but introduces infrastructure management with no benefit. |
| **dbt over raw SQL scripts** | Version-controlled, testable, self-documenting transformations. Every model is a node in a DAG with lineage you can inspect. |
| **Africa/Lagos timezone for daily grain** | Daily rollups bucketed in UTC would misalign with local operational decisions. `DATE(observed_at, 'Africa/Lagos')` ensures the date means what a Nigerian operator would expect it to mean. |

---

## Real-World Challenges Handled

This is not a clean-dataset tutorial. The pipeline was designed with production constraints in mind:

### 1. Duplicate hours from re-runs
The Open-Meteo forecast API returns overlapping windows — re-running ingestion inserts duplicate rows for the same hour. The staging model handles this with a deduplication strategy: for any given `observed_at`, only the latest `ingested_at` row survives. Downstream models always see exactly one row per hour.

```sql
-- Deduplication in stg_weatherdata_raw
qualify row_number() over (
    partition by observed_at
    order by ingested_at desc
) = 1
```

### 2. Legacy column naming (schema drift)
The raw table was initially loaded with a typo in the column name (`temprature_c` instead of `temperature_c`). Rather than requiring a manual fix before the pipeline could run, a feature flag in `dbt_project.yml` handles both states:

```yaml
# dbt_project.yml
vars:
  legacy_raw_temperature_column: true   # set false after column rename
```

The staging model reads the right column name based on this flag. This simulates a real-world schema evolution scenario and shows the pipeline degrades gracefully rather than breaking.

### 3. Forecast data, not observations
The Open-Meteo API returns **forecast** data, not historical station observations. This distinction matters — forecast values for a past hour may differ from what actually occurred. The mart is named `mart_daily_forecast` deliberately, and this is documented in the data model so any downstream consumer understands what they're querying.

---

## Data Models

### `stg_weatherdata_raw` (view)

Reads from `weather_data.abuja_hourly`. Applies:
- Type casting on all columns
- Null filter on `observed_at`
- Deduplication by `observed_at` (latest ingestion wins)
- Derives `date_day` in Africa/Lagos timezone

### `mart_daily_forecast` (table)

Daily grain. One row per Lagos calendar date.

| Column | Description |
|---|---|
| `date_day` | Calendar date (Africa/Lagos) |
| `avg_temp_c` | Mean hourly temperature |
| `max_temp_c` | Peak temperature |
| `min_temp_c` | Minimum temperature |
| `total_precipitation_mm` | Sum of hourly precipitation |
| `max_windspeed_kmh` | Peak wind speed |
| `logistics_risk_level` | `Low Risk` / `Moderate Risk` / `High Risk` |

**Risk logic:**
```sql
case
    when total_precipitation_mm > 20 then 'High Risk'
    when total_precipitation_mm > 5  then 'Moderate Risk'
    else 'Low Risk'
end as logistics_risk_level
```

---

## Data Quality

Tests are defined in `models/schema.yml` and enforced with `dbt test`:

| Test | Column | Model |
|---|---|---|
| `not_null` | `observed_at`, `temperature_c`, `precipitation_mm` | Staging |
| `unique` | `observed_at` | Staging |
| `not_null` | `date_day` | Mart |
| `unique` | `date_day` | Mart |
| `accepted_values` | `logistics_risk_level` | Mart |
| Source freshness | `ingested_at` on `raw.abuja_hourly` | Source |

Source freshness is configured in `_sources.yml` — `dbt source freshness` will warn if the raw table hasn't received data within a defined window, catching silent ingestion failures before they reach the mart.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Data source | [Open-Meteo API](https://open-meteo.com/) (free, no auth required) |
| Ingestion | Python 3.13, `requests`, `google-cloud-bigquery` |
| Warehouse | Google BigQuery |
| Transformation | dbt Core + dbt-bigquery |
| Auth | GCP Service Account (JSON key, never committed) |

---

## Repository Layout

```
weatherdataabuja/
├── ingest.py                               # Fetch + stream to BigQuery
├── requirements.txt
├── dbt_project.yml                         # dbt config + feature flags
├── models/
│   ├── staging/
│   │   ├── stg_weatherdata_raw.sql
│   │   └── _sources.yml                   # Raw source + freshness config
│   ├── mart/
│   │   └── mart_daily_forecast.sql
│   └── schema.yml                          # Column docs + generic tests
├── analyses/
│   └── rename_raw_temperature_column.sql  # One-time DDL for column rename
└── weather_abuja.csv                       # Sample export for reference
```

---

## Getting Started

### Prerequisites

- Google Cloud project with BigQuery enabled
- Service account with `BigQuery Data Editor` on the target dataset
- Service account JSON key (**never commit this**)
- Python 3.11+

### 1. Clone and install

```bash
git clone https://github.com/<your-username>/weatherdataabuja.git
cd weatherdataabuja
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Set credentials

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/absolute/path/to/your-service-account.json"

# Optional: override the default destination table
export BIGQUERY_TABLE_ID="your-project.your_dataset.abuja_hourly"
```

### 3. Configure dbt profile

Create or update `~/.dbt/profiles.yml`:

```yaml
weather_abuja_pl:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: your-gcp-project-id
      dataset: weather_data
      location: US
      keyfile: /absolute/path/to/your-service-account.json
      threads: 4
```

Update `database` and `schema` in `models/staging/_sources.yml` to match your project and dataset.

### 4. Handle the legacy column (if applicable)

If your raw table still has `temprature_c` (the typo), leave `legacy_raw_temperature_column: true` in `dbt_project.yml`. After renaming the column (see `analyses/rename_raw_temperature_column.sql`), set it to `false`.

### 5. Run the pipeline

```bash
# Ingest
python ingest.py

# Transform
dbt run

# Test
dbt test

# Check source freshness
dbt source freshness
```

---

## What's Next

The current state of the project is a working ingestion and transformation layer with tests. The planned next phases are:

**Orchestration** — wrapping the pipeline in an Airflow DAG (via Astronomer) so ingestion and dbt runs are scheduled, monitored, and retried automatically. A cron job would work at this scale, but Airflow gives visibility into failures and makes the dependency chain (`ingest → dbt run → dbt test`) explicit.

**Pre-load data quality with Great Expectations** — validating the API response *before* it touches BigQuery. The current dbt tests catch problems after load; GE would add a gate at the ingestion boundary, which is the right place to stop bad data early.

**BI layer** — a Looker Studio dashboard connected to `mart_daily_forecast`, surfacing the logistics risk signal visually. The mart is already structured for this; the dashboard is the delivery mechanism for the business value the pipeline produces.

**Incremental models** — as the raw table grows, a full-refresh mart becomes expensive. The next modeling step is converting `mart_daily_forecast` to an incremental model that only reprocesses recent data.

---

## Author

Built as a data engineering and analytics engineering portfolio project.
Covers the full analytical stack: REST ingestion → cloud warehouse → transformation layer → declarative testing → business-interpretable output.
