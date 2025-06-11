import requests
import time
import os
import json
from datetime import datetime

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "your_project")
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID", "your_dataset")
BQ_SPEED_TABLE_ID = os.getenv("BQ_SPEED_TABLE_ID", "internet_speed")

def query_prometheus(query):
    url = f"{PROMETHEUS_URL}/api/v1/query"
    response = requests.get(url, params={"query": query})
    response.raise_for_status()
    return response.json()

def extract_latest_value(metric_name):
    try:
        result = query_prometheus(metric_name)
        results = result.get("data", {}).get("result", [])
        if results:
            val = results[0]["value"][1]
            return float(val)
    except Exception as e:
        print(f"Error querying {metric_name}: {e}")
    return None
metrics = [
    "scrape_duration_seconds",
    "scrape_samples_scraped",
    "scrape_series_added",
    "node_netstat_TcpExt_SyncookiesFailed"
]
def push_speedtest_metrics():
    ts = int(time.time())

    download = extract_latest_value("speedtest_download_bits_per_second")
    upload = extract_latest_value("speedtest_upload_bits_per_second")
    ping_ms = extract_latest_value("speedtest_ping_latency_milliseconds")
    for m in metrics:
        print(f"\n⏱ Querying {m}...")
        res = query_prometheus(m)
        for r in res.get("data", {}).get("result", []):
            metric = r["metric"]
            val = r["value"][1]
            print(f"{metric.get('job', '')}@{metric.get('instance', '')} → {val}")
    if download is None or upload is None or ping_ms is None:
        print(f"{download}  up {upload}  ping {ping_ms}.")
        print("Incomplete metrics, skipping push.")
        return

    row = {
        "timestamp": ts,
        "datetime": datetime.utcfromtimestamp(ts).isoformat() + "Z",
        "download_bps": download,
        "upload_bps": upload,
        "ping_ms": ping_ms,
        "location": "Thailand",
        "site_id": "Isaac's house"
    }

    print(f"\n🏓 Hourly Speedtest Push to BigQuery: {BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_SPEED_TABLE_ID}")
    print(json.dumps(row, indent=2))
    # TODO: replace with actual BigQuery insert call
    # bq_client.insert_rows_json(...)

def main():
    print("🔁 Running hourly speedtest push loop. Press Ctrl+C to stop.")
    while True:
        push_speedtest_metrics()
        time.sleep(3600)  # Sleep for 1 hour

if __name__ == "__main__":
    main()

