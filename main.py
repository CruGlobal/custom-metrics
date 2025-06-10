import requests
from google.cloud import bigquery
from google.oauth2 import service_account
import time
import os

# Configuration
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID", "internet_metrics")
BQ_TABLE_ID_up = os.getenv("BQ_TABLE_ID", "uptime")
BQ_TABLE_ID_speed = os.getenv("BQ_TABLE_ID", "speedtest")
metrics = [
    "scrape_duration_seconds",
    "scrape_samples_scraped",
    "scrape_series_added",
    "node_netstat_TcpExt_SyncookiesFailed"
]

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
    values = []
    ts = int(time.time())
    for r in results:
        metric = r.get("metric", {})
        value = r.get("value", [None, None])[1]
        values.append({
            "timestamp": ts,
            "metric": f"{metric.get('job', 'unknown')}_{metric.get('instance', 'unknown')}_up",
            "site_id": metric.get("site_id", "Isaac's house"),
            "location": metric.get("location", "Thailand"),
            "instance": metric.get("instance"),
            "job": metric.get("job"),
            "data": r,
            "value": f"{str(value)}"
        })  
    sample = results[0] if results else {}
    if not sample:
        print("No results found in Prometheus response.")
        return {}
    row = [ {
            "timestamp": ts,
            "metric": "up",
            "site_id": (sample.get(0, {}).get("metric", {}).get("site_id", "Isaac's house")),
            "location": sample.get(0, {}).get("metric", {}).get("location", "Thailand"),
            "instance": sample.get(0, {}).get("metric", {}).get("instance"),
            "job": "prometheus",
            "value": f"{str(results)}"
        }]
    return row

def parse_speed(result_json, metric):
    """Extracts desired fields from Prometheus response and prepares rows for BigQuery."""
    results = result_json.get("data", {}).get("result", [])
    values = []
    ts = int(time.time())
    for r in results:
        value = r.get("value", [None, None])[1]
        values.append({
            "timestamp": ts,
            "metric": metric,
            "data": r,
            "value": f"{str(value)}"
        })  
    row = [ {
            "timestamp": ts,
            "metric": "speed",
            "job": "prometheus",
            "value": f"{str(results)}"
        }]
    return row

def push_to_bigquery(rows, table_id=None):
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_id}"
    errors = bq_client.insert_rows_json(table_id, rows)
    if errors:
        print("Encountered errors while inserting rows:", errors)
    else:
        print(f"Inserted {len(rows)} rows.")

def main():
    print("Pulling metrics from Prometheus...")
    prom_data = query_prometheus("up")
    rows = parse_and_format(prom_data)
    speed = []
    for m in metrics:
        res = query_prometheus(m)
        for r in res.get("data", {}).get("result", []):
            metric = r["metric"]
            val = r["value"][1]
            print(f"{metric.get('job', '')}@{metric.get('instance', '')} → {val}")
        speed.append( (res, m) )
    if rows:
        print(f"Pushing {len(rows)} rows to BigQuery...")
        push_to_bigquery(rows, BQ_TABLE_ID_up)
    else:
        print("No uptime data found.")
    if speed:
        print(f"Pushing {len(rows)} speed rows to BigQuery...")
        for s in speed:
            rows = parse_speed(s[0], s[1])
            if rows:
               push_to_bigquery(rows, BQ_TABLE_ID_speed)
            else:
                print("No speed data found.")

if __name__ == "__main__":
    main()
