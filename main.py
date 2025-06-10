import requests
from google.cloud import bigquery
from google.oauth2 import service_account
import time
import os

# Configuration
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID", "internet_metrics")
BQ_TABLE_ID = os.getenv("BQ_TABLE_ID", "uptime")

# BigQuery Credentials (loaded from mounted secret)
credentials = service_account.Credentials.from_service_account_file(
    os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/config/credentials.json")
)
bq_client = bigquery.Client(credentials=credentials, project=BQ_PROJECT_ID)

def query_prometheus(query):
    url = f"{PROMETHEUS_URL}/api/v1/query"
    response = requests.get(url, params={"query": query})
    response.raise_for_status()
    return response.json()

def parse_and_format(result_json):
    """Extracts desired fields from Prometheus response and prepares rows for BigQuery."""
    results = result_json.get("data", {}).get("result", [])
    rows = []
    ts = int(time.time())
    for r in results:
        metric = r.get("metric", {})
        value = r.get("value", [None, None])[1]
        rows.append({
            "timestamp": ts,
            "metric": f"{metric.get('job', 'unknown')}_{metric.get('instance', 'unknown')}_up",
            "site_id": metric.get("site_id", "Isaac's house"),
            "location": metric.get("location", "Thailand"),
            "instance": metric.get("instance"),
            "job": metric.get("job"),
            "value": f"{value}"
        })
    return rows

def push_to_bigquery(rows):
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"
    errors = bq_client.insert_rows_json(table_id, rows)
    if errors:
        print("Encountered errors while inserting rows:", errors)
    else:
        print(f"Inserted {len(rows)} rows.")

def main():
    print("Pulling metrics from Prometheus...")
    prom_data = query_prometheus("up")
    rows = parse_and_format(prom_data)
    if rows:
        print(f"Pushing {len(rows)} rows to BigQuery...")
        push_to_bigquery(rows)
    else:
        print("No data found.")

if __name__ == "__main__":
    main()
