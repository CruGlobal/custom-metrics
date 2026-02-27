import os
import uuid
import logging
import schedule
import datetime
import requests
import asyncio
from submit_to_google_form import ping, speed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ! This can be mapped in the internet pi stack "/etc/network-monitor/site_id"
LOCATION = os.getenv("LOCATION", "Isenguard")
SITE_ID_FILE = "network-monitor/site_id"

# Ping metrics to collect every 5 minutes
PING_METRICS = {
    "google_up": 'up{job="ping", instance="http://www.google.com/"}',
    "apple_up": 'up{job="ping", instance="https://www.apple.com/"}',
    "github_up": 'up{job="ping", instance="https://github.com/"}',
    "cru_up": 'up{job="ping", instance="https://cru.org/"}',
    "netsuite_up": 'up{job="ping", instance="https://netsuite.cru.org/"}',
    "okta_up": 'up{job="ping", instance="https://www.okta.com/"}',
    "pihole_up": 'up{job="pihole", instance="pihole-exporter:9617"}',
    "node_up": 'up{job="node", instance="nodeexp:9100"}',
    "speedtest_up": 'up{job="speedtest", instance="speedtest:9798"}',
    "http_latency": 'probe_http_duration_seconds{job="ping", phase="connect"}',
    "http_samples": "scrape_samples_scraped{job='ping'}",
    "http_time": "scrape_duration_seconds{job='ping'}",
    "http_content_length": 'probe_http_uncompressed_body_length{job="ping"}',
    "http_duration": 'probe_duration_seconds{job="ping"}'
}

# Speedtest metrics to collect when available
SPEED_METRICS = {
    "download_mbps": 'speedtest_download_bits_per_second{job="speedtest"}',
    "upload_mbps": 'speedtest_upload_bits_per_second{job="speedtest"}',
    "ping_ms": 'speedtest_ping_latency_milliseconds{job="speedtest"}',
    "jitter_ms": 'speedtest_jitter_latency_milliseconds{job="speedtest"}'
}

class NetworkMonitor:
    def __init__(self):
        self.site_id = "TEMP_TEST_DATA"
        self.location = LOCATION
        
    def _get_or_create_site_id(self):
        """Get existing site ID or create a new one."""
        if os.path.exists(SITE_ID_FILE):
            with open(SITE_ID_FILE, 'r') as f:
                return f.read().strip()
        else:
            site_id = str(uuid.uuid4())
            os.makedirs(os.path.dirname(SITE_ID_FILE), exist_ok=True)
            with open(SITE_ID_FILE, 'w') as f:
                f.write(site_id)
            return site_id

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
            logger.info(f"Failed to query Prometheus: {e} at {prometheus_url}")
            return None

    def _insert_ping_metrics(self, metrics_data):
        """Insert ping metrics directly into the sheet."""
        try:
            # Add site_id and location to metrics_data
            metrics_data["site_id"] = self.site_id
            metrics_data["location"] = self.location # Pass only the location string
            
            ping(metrics_data)
            # logger.info(f"Successfully submitted {len(metrics_data)} ping metrics to Google Form")
        except Exception as e:
            logger.info(f"Error submitting ping metrics to Google Form: {e}")

    def _insert_speed_metrics(self, metrics_data):
        """Insert speedtest metrics directly into the form."""
        try:
            # Add site_id and location to metrics_data
            metrics_data["site_id"] = self.site_id
            metrics_data["location"] = self.location # Pass only the location string
            
            speed(metrics_data)
            # logger.info(f"Successfully submitted {len(metrics_data)} speed metrics to Google Form")

        except Exception as e:
            logger.info(f"Error submitting speed metrics to Google Form: {e}")

    def collect_ping_metrics(self):
        """Collect and store ping metrics."""
        # logger.info("Collecting ping metrics...")
        metrics_data = {}
        
        for metric_name, query in PING_METRICS.items():
            result = self._query_prometheus(query)
            if result and 'data' in result and 'result' in result['data']:
                for r in result['data']['result']:
                    value = float(r['value'][1])
                    # Convert 'up' metrics to integer (0 or 1)
                    if metric_name in ["google_up", "apple_up", "github_up", "pihole_up", "node_up", "speedtest_up"]:
                        metrics_data[metric_name] = int(value)
                    else:
                        metrics_data[metric_name] = value
        
        if metrics_data:
            self._insert_ping_metrics(metrics_data)

    def collect_speed_metrics(self):
        """Collect and store speedtest metrics if available."""
        # logger.info("Checking for speedtest metrics...")
        metrics_data = {}
        
        # First check if speedtest is up
        speedtest_up_result = self._query_prometheus(SPEED_METRICS["download_mbps"])
        if not speedtest_up_result or 'data' not in speedtest_up_result or not speedtest_up_result['data']['result']:
            logger.info(f"No speedtest data found")
            return
        
        metrics_data["site_id"] = self.site_id
        metrics_data["location"] = self.location # Pass only the location string
        
        for metric_name, query in SPEED_METRICS.items():
            result = self._query_prometheus(query)
            if result and 'data' in result and 'result' in result['data']:
                for r in result['data']['result']:
                    value = float(r['value'][1])
                    # Convert bits to Mbps for speed metrics
                    if metric_name in ['download_mbps', 'upload_mbps']:
                        metrics_data[metric_name] = value / 1_000_000
                    else:
                        metrics_data[metric_name] = value
        if metrics_data:
            # logger.info(f"Found speedtest data {metrics_data}")
            self._insert_speed_metrics(metrics_data)
    


async def main():
    logger.info(f"Starting main.py in custom-metrics")

    monitor = NetworkMonitor()
    
    # Schedule ping metrics collection every 5 minutes
    schedule.every(5).minutes.do(monitor.collect_ping_metrics)
    
    # Schedule speedtest metrics collection every 60 minutes
    # (it will only insert data if new speedtest results are available)
    # schedule.every(60).minutes.do(monitor.collect_speed_metrics)
    schedule.every(60).minutes.do(monitor.collect_speed_metrics)

    
    # Run initial collection
    monitor.collect_ping_metrics()
    monitor.collect_speed_metrics()
    
    # Keep the script running
    while True:
        await asyncio.sleep(1)
        schedule.run_pending()

if __name__ == "__main__":
    asyncio.run(main())
