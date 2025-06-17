import requests
import time
import os
from datetime import datetime
import json

# Configuration
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
SITE_ID = os.getenv("SITE_ID", "default_site")  # Unique identifier for this device

def query_prometheus(query, opt=PROMETHEUS_URL):
    url = f"{opt}/api/v1/query"
    response = requests.get(url, params={"query": query})
    response.raise_for_status()
    return response.json()

def list_available_metrics():
    """List all available metrics for the ping job."""
    print("\nAvailable metrics for ping job:")
    result = query_prometheus('{job="ping"}')
    metrics = set()
    for r in result.get("data", {}).get("result", []):
        metric_name = r["metric"].get("__name__", "")
        if metric_name:
            metrics.add(metric_name)
    
    for metric in sorted(metrics):
        print(f"  {metric}")

def collect_metrics():
    # """Collect all metrics for this device."""
    # list_available_metrics()
    
    metrics_data = {
        "timestamp": int(time.time()),
        "site_id": SITE_ID,
        "download_speed": 0,
        "upload_speed": 0,
        "latency": 0,
        "jitter": 0,
        "samples": 0,
        "time": 0,
        "content_length": 0,
        "duration": 0,
        # Add up status metrics
        "google_up": 0,
        "apple_up": 0,
        "github_up": 0,
        "pihole_up": 0,
        "node_up": 0,
        "speedtest_up": 0
    }
    
    # Define metrics to collect
    metrics = {
        "google_up": 'up{job="ping", instance="http://www.google.com/"}',
        "apple_up": 'up{job="ping", instance="https://www.apple.com/"}',
        "github_up": 'up{job="ping", instance="https://github.com/"}',
        "pihole_up": 'up{job="pihole", instance="pihole-exporter:9617"}',
        "node_up": 'up{job="node", instance="nodeexp:9100"}',
        "speedtest_up": 'up{job="speedtest", instance="speedtest:9798"}',
        "time": "scrape_duration_seconds{job='ping'}",
        "samples": "scrape_samples_scraped{job='ping'}",
        "latency": 'probe_http_duration_seconds{job="ping", phase="connect"}',
        "content_length": 'probe_http_uncompressed_body_length{job="ping"}',
        "duration": 'probe_duration_seconds{job="ping"}',
        "download_speed": "speedtest_download_bits_per_second{}",
        "upload_speed": "speedtest_upload_bits_per_second{}",
        "jitter": "speedtest_jitter_latency_milliseconds{}"
    }
    
    # Collect metrics
    for metric_name, query in metrics.items():
        result = query_prometheus(query)
        for r in result.get("data", {}).get("result", []):
            value = float(r["value"][1])
            metrics_data[metric_name] = value
    
    # Calculate download speed in Mbps if we have the data
    if metrics_data["duration"] > 0 and metrics_data["content_length"] > 0:
        download_speed = (metrics_data["content_length"] / (1024 * 1024 * 8)) / metrics_data["duration"]
        metrics_data["download_speed"] = download_speed
    
    return metrics_data

def preview_metrics(record):
    """Format and print record for preview."""
    print("\n=== Preview of metrics that would be pushed to BigQuery ===")
    print(f"Site ID: {record['site_id']}")
    print(f"Timestamp: {datetime.fromtimestamp(record['timestamp'])}")
    
    print("\nService Status:")
    print(f"  Google: {'Up' if record['google_up'] == 1 else 'Down'}")
    print(f"  Apple: {'Up' if record['apple_up'] == 1 else 'Down'}")
    print(f"  GitHub: {'Up' if record['github_up'] == 1 else 'Down'}")
    print(f"  PiHole: {'Up' if record['pihole_up'] == 1 else 'Down'}")
    print(f"  Node Exporter: {'Up' if record['node_up'] == 1 else 'Down'}")
    print(f"  Speedtest: {'Up' if record['speedtest_up'] == 1 else 'Down'}")
    
    print("\nPerformance Metrics:")
    print(f"  Download Speed: {record['download_speed']:.2f} Mbps")
    print(f"  Upload Speed: {record['upload_speed']:.2f} Mbps")
    print(f"  Latency: {record['latency']:.2f} seconds")
    print(f"  Jitter: {record['jitter']:.2f} ms")
    print(f"  Samples: {record['samples']}")
    print(f"  Time: {record['time']:.2f} seconds")
    print(f"  Content Length: {record['content_length']} bytes")
    print(f"  Duration: {record['duration']:.2f} seconds")
    
    print("\n=====================================")

def main():
    try:
        # Collect all metrics
        record = collect_metrics()
        
        # Preview metrics
        print(f"\nPreviewing metrics for site {SITE_ID} at {datetime.fromtimestamp(record['timestamp'])}")
        preview_metrics(record)
        
    except Exception as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    main()

"""
BigQuery Schema:
CREATE TABLE `animated-bay-462107-q2.internet_monitoring1.tablev1` (
  timestamp TIMESTAMP,
  site_id STRING,
  download_speed FLOAT,
  upload_speed FLOAT,
  latency FLOAT,
  jitter FLOAT,
  samples FLOAT,
  time FLOAT,
  content_length FLOAT,
  duration FLOAT,
  google_up FLOAT,
  apple_up FLOAT,
  github_up FLOAT,
  pihole_up FLOAT,
  node_up FLOAT,
  speedtest_up FLOAT
)
"""