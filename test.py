import requests
import time
import os
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
PROMETHEUS_speed = os.getenv("PROMETHEUS_URL", "http://localhost:9798")

def query_prometheus(query, opt=PROMETHEUS_URL):
    url = f"{opt}/api/v1/query"
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
            "instance": metric.get("instance"),
            "job": metric.get("job"),
            "value": float(value)
        })
    return rows

def push_to_bigquery(rows):
    print(rows)

print("Pulling metrics from Prometheus...")
prom_data = query_prometheus("up")
prom_speed_data = query_prometheus("up", PROMETHEUS_speed)
rows = parse_and_format(prom_data)
rows1 = parse_and_format(prom_speed_data)
print(f"Pushing {len(rows)} rows to BigQuery...")
push_to_bigquery(rows)
push_to_bigquery(rows1)
