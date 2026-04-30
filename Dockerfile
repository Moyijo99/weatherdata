FROM astrocrpublic.azurecr.io/runtime:3.1-14

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
        dbt-core \
        dbt-bigquery