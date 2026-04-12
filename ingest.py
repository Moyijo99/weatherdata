import requests
from datetime import datetime
from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/usr/local/airflow/meteo-ingest-ce3765c5326f.json"

TABLE_ID = "meteo-ingest.weather_data.abuja_hourly"

def fetch_data():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 9.0765,
        "longitude": 7.3986,
        "hourly": "temperature_2m,precipitation,windspeed_10m",
        "timezone": "Africa/Lagos"
    }
    response = requests.get(url,params=params)
    response.raise_for_status()
    return response.json()

def flatten_to_rows(data):
    hourly = data["hourly"]
    rows = []
    for i, time in enumerate(hourly["time"]):
        rows.append({
            "time": time,
            "temperature_c": hourly["temperature_2m"][i],
            "precipitation_mm": hourly["precipitation"][i],
            "windspeed_kmh": hourly["windspeed_10m"][i],
            "ingested_at": datetime.now().isoformat(),
        })
    return rows

    
def dump_to_bigquery(rows):
    client = bigquery.Client()
    
    errors = client.insert_rows_json(TABLE_ID, rows)
    if errors:
        print(f"Encountered errors while inserting rows: {errors}")
    else:
        print(f"Inserted {len(rows)} rows into Table {TABLE_ID}")


if __name__ == "__main__":
    data = fetch_data()
    rows = flatten_to_rows(data)
    dump_to_bigquery(rows)
    print(f"Data ingested and saved to {TABLE_ID}")