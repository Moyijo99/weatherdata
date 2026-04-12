from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_ROOT = os.getenv("WEATHER_PROJECT_ROOT", "/usr/local/airflow")
DBT_PROJECT_DIR = PROJECT_ROOT
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/usr/local/airflow/.dbt")


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="weather_abuja_pipeline",
    description="Fetch forecast data, run dbt models, then run dbt tests.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",  # hourly
    catchup=False,
    max_active_runs=1,
    tags=["weather", "bigquery", "dbt"],
) as dag:
    fetch_and_load = BashOperator(
        task_id="fetch_and_load",
        bash_command=f"python {PROJECT_ROOT}/ingest.py",
        env={
            **os.environ,
            "GOOGLE_APPLICATION_CREDENTIALS": os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/usr/local/airflow/meteo-ingest-ce3765c5326f.json"),
            "BIGQUERY_TABLE_ID": os.getenv(
                "BIGQUERY_TABLE_ID",
                "meteo-ingest.weather_data.abuja_hourly",
            ),
        },
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} "
            "--select stg_weatherdata_raw mart_daily_forecast"
        ),
        env={**os.environ},
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} "
            "--select stg_weatherdata_raw mart_daily_forecast"
        ),
        env={**os.environ},
    )

    fetch_and_load >> dbt_run >> dbt_test
