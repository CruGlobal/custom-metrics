import os
import time
import json
import uuid
import logging
import schedule
import requests
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SITE_ID_FILE = "/etc/network-monitor/site_id"
BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT", "your-project-id")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "internet_monitoring1")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE", "network_metrics")

# Metrics to collect
METRICS = [
    "scrape_duration_seconds",
    "scrape_samples_scraped",
    "scrape_series_added",
    "node_netstat_TcpExt_SyncookiesFailed"
]

class NetworkMonitor:
    def __init__(self):
        LOCATION = os.getenv("LOCATION", "unknown")
        self.site_id = self._get_or_create_site_id()
        self.bigquery_client = self._init_bigquery_client()
        self.location = LOCATION
        
    def _get_or_create_site_id(self):
        """Get existing site ID or create a new one."""
        if os.path.exists(SITE_ID_FILE):
            with open(SITE_ID_FILE, 'r') as f:
                return f.read().strip()
        else:
            site_id = str(uuid.uuid4())
            with open(SITE_ID_FILE, 'w') as f:
                f.write(site_id)
            return site_id

    def _init_bigquery_client(self):
        """Initialize BigQuery client with service account credentials."""
        try:
            return bigquery.Client()
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            raise

    def _query_prometheus(self, query):
        """Query Prometheus for metrics."""
        try:
            prometheus_url = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
            response = requests.get(
                f"{prometheus_url}/api/v1/query",
                params={"query": query}
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to query Prometheus: {e}")
            return None

    def _insert_metric(self, metric_data):
        """Insert a metric into BigQuery."""
        try:
            row = {
                'timestamp': datetime.utcnow().isoformat(),
                'site_id': self.site_id,
                'metric': metric_data.get('metric', 'unknown'),
                'job': metric_data.get('job', 'network_monitor'),
                'location': self.location,
                'value': str(metric_data.get('value', '')),
                'instance': metric_data.get('instance', 'raspberry_pi'),
                'google_ping': metric_data.get('google_ping'),
                'google_time': metric_data.get('google_time'),
                'google_samples': metric_data.get('google_samples'),
                'apple_ping': metric_data.get('apple_ping'),
                'apple_time': metric_data.get('apple_time'),
                'apple_samples': metric_data.get('apple_samples'),
                'github_ping': metric_data.get('github_ping'),
                'github_time': metric_data.get('github_time'),
                'github_samples': metric_data.get('github_samples')
            }
            
            table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
            errors = self.bigquery_client.insert_rows_json(table_id, [row])
            
            if errors:
                logger.error(f"Failed to insert row: {errors}")
            else:
                logger.info(f"Successfully inserted {metric_data.get('metric')} metric")
                
        except Exception as e:
            logger.error(f"Error inserting metric: {e}")

    def _parse_prometheus_result(self, result, metric_name):
        """Parse Prometheus query result into metric data."""
        if not result or 'data' not in result or 'result' not in result['data']:
            return None

        metrics_data = {}
        for r in result['data']['result']:
            metric = r.get('metric', {})
            value = r.get('value', [None, None])[1]
            
            # Extract ping metrics if available
            if 'ping_target' in metric:
                target = metric['ping_target']
                if target in ['google', 'apple', 'github']:
                    metrics_data.update({
                        f'{target}_ping': value == '1.0',
                        f'{target}_time': float(metric.get('ping_time', 0)),
                        f'{target}_samples': float(metric.get('ping_samples', 0))
                    })

        return {
            'metric': metric_name,
            'value': str(value) if 'value' in locals() else '{}',
            'instance': metric.get('instance', 'raspberry_pi'),
            'job': metric.get('job', 'network_monitor'),
            **metrics_data
        }

    def collect_metrics(self):
        """Collect all metrics from Prometheus."""
        logger.info("Pulling metrics from Prometheus...")
        
        # Get basic up status
        up_data = self._query_prometheus("up")
        if up_data:
            metric_data = self._parse_prometheus_result(up_data, 'up')
            if metric_data:
                self._insert_metric(metric_data)

        # Get all configured metrics
        for metric in METRICS:
            result = self._query_prometheus(metric)
            if result:
                metric_data = self._parse_prometheus_result(result, metric)
                if metric_data:
                    self._insert_metric(metric_data)

def main():
    monitor = NetworkMonitor()
    
    # Schedule metric collection
    schedule.every(5).minutes.do(monitor.collect_metrics)
    
    # Run initial collection
    monitor.collect_metrics()
    
    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
