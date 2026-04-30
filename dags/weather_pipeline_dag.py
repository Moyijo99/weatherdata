from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig

PROJECT_ROOT = os.getenv("WEATHER_PROJECT_ROOT", "/usr/local/airflow")
DBT_PROJECT_PATH = Path("/usr/local/airflow/include/dbt")
DBT_EXECUTABLE_PATH = Path("/usr/local/airflow/dbt_venv/bin/dbt")
DBT_PROFILES_FILE_PATH = Path("/usr/local/airflow/include/dbt/profiles.yml")



default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

profile_config = ProfileConfig(
    profile_name="weather_abuja_pl",
    target_name="dev",
    profiles_yml_filepath=DBT_PROFILES_FILE_PATH,
)

with DAG(
    dag_id="weather_abuja_pipeline",
    description="Fetch forecast data, run dbt models and tests via Cosmos.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["weather", "bigquery", "dbt", "cosmos"],
) as dag:

    fetch_and_load = BashOperator(
        task_id="fetch_and_load",
        bash_command=f"python {PROJECT_ROOT}/ingest.py",
        env={
            **os.environ,
            "GOOGLE_APPLICATION_CREDENTIALS": os.getenv(
                "GOOGLE_APPLICATION_CREDENTIALS",
                "/usr/local/airflow/meteo-ingest-ce3765c5326f.json",
            ),
            "BIGQUERY_TABLE_ID": os.getenv(
                "BIGQUERY_TABLE_ID",
                "meteo-ingest.weather_data.abuja_hourly",
            ),
        },
    )

    dbt_pipeline = DbtTaskGroup(
        group_id="dbt_weather_pipeline",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
    )

    fetch_and_load >> dbt_pipeline
