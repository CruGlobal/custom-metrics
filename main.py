import os
import time
import json
import uuid
import logging
import schedule
import requests
from datetime import datetime
import database
from datetime import datetime
from turso_python.connection import TursoConnection
from turso_python.crud import TursoCRUD
from turso_python.batch import TursoBatch

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ! This may be mapped in the internet pi stack "/etc/network-monitor/site_id"
SITE_ID_FILE = "network-monitor/site_id"
PING_TABLE = os.getenv("PING_TABLE", "ping")
SPEED_TABLE = os.getenv("SPEED_TABLE", "speed")

# Ping metrics to collect every 5 minutes
PING_METRICS = {
    "google_up": 'up{job="ping", instance="http://www.google.com/"}',
    "apple_up": 'up{job="ping", instance="https://www.apple.com/"}',
    "github_up": 'up{job="ping", instance="https://github.com/"}',
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
        LOCATION = os.getenv("LOCATION", "unknown")
        self.site_id = self._get_or_create_site_id()
        self.location = LOCATION
        sync_interval_str = os.getenv("SYNC_INTERVAL", "50")
        try:
            self.sync_interval_minutes = int(sync_interval_str) if sync_interval_str else 1440
        except ValueError:
            logger.warning(f"Invalid SYNC_INTERVAL '{sync_interval_str}'. Defaulting to 1440 minutes.")
            self.sync_interval_minutes = 1440
        
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
            logger.error(f"Failed to query Prometheus: {e}")
            return None

    async def _mass_upload_metrics(self):
        """Upload all cached metrics from SQLite to Turso."""
        try:
            crud = database.get_turso_crud()
            
            # Get all ping metrics from SQLite
            ping_metrics = database.get_all_ping_metrics()
            
            if ping_metrics:
                batch = TursoBatch(crud.connection)
                batch.batch_insert("ping_metrics", ping_metrics)
                logger.info(f"Successfully inserted {len(ping_metrics)} ping metrics into Turso.")
                database.clear_ping_metrics()
            
            # Get all speed metrics from SQLite
            speed_metrics = database.get_all_speed_metrics()
            
            if speed_metrics:
                batch = TursoBatch(crud.connection)
                batch.batch_insert("speed_metrics", speed_metrics)
                logger.info(f"Successfully inserted {len(speed_metrics)} speed metrics into Turso.")
                database.clear_speed_metrics()
        except Exception as e:
            logger.error(f"Error uploading metrics to Turso: {e}")

    def _ensure_float_values(self, metrics_data):
        """Ensure all numeric values in metrics_data are floats."""
        for key, value in metrics_data.items():
            if isinstance(value, str):
                try:
                    metrics_data[key] = float(value)
                except ValueError:
                    pass  # Keep as string if not a valid float
        return metrics_data

    def _insert_ping_metrics(self, metrics_data):
        """Insert ping metrics into SQLite database."""
        try:
            # Add site_id and location to metrics_data
            metrics_data['site_id'] = self.site_id
            metrics_data['location'] = self.location
            
            # Ensure all numeric values are floats
            metrics_data = self._ensure_float_values(metrics_data)

            # Insert into SQLite database
            database.insert_ping_metrics(metrics_data)
            logger.info("Successfully inserted ping metrics into SQLite")
        except Exception as e:
            logger.error(f"Error inserting ping metrics into SQLite: {e}")

    def _insert_speed_metrics(self, metrics_data):
        """Insert speedtest metrics into SQLite database."""
        try:
            # Add site_id and location to metrics_data
            metrics_data['site_id'] = self.site_id
            metrics_data['location'] = self.location
            
            # Ensure all numeric values are floats
            metrics_data = self._ensure_float_values(metrics_data)

            # Insert into SQLite database
            database.insert_speed_metrics(metrics_data)
            logger.info("Successfully inserted speed metrics into SQLite")
        except Exception as e:
            logger.error(f"Error inserting speed metrics into SQLite: {e}")

    async def upload_metrics(self):
        """Collect and upload metrics."""
        logger.info("Uploading metrics...")
        await self._mass_upload_metrics()

    def collect_ping_metrics(self):
        """Collect and store ping metrics."""
        logger.info("Collecting ping metrics...")
        metrics_data = {}
        
        for metric_name, query in PING_METRICS.items():
            result = self._query_prometheus(query)
            if result and 'data' in result and 'result' in result['data']:
                for r in result['data']['result']:
                    value = float(r['value'][1])
                    # Convert bits to Mbps for speed metrics
                    if metric_name in ['download_mbps', 'upload_mbps']:
                        value = value / 1_000_000
                    metrics_data[metric_name] = value
        
        if metrics_data:
            self._insert_ping_metrics(metrics_data)

    def collect_speed_metrics(self):
        """Collect and store speedtest metrics if available."""
        logger.info("Checking for speedtest metrics...")
        metrics_data = {}
        
        # First check if speedtest is up
        speedtest_up = self._query_prometheus(SPEED_METRICS['download_mbps'])
        if not speedtest_up or 'data' not in speedtest_up or not speedtest_up['data']['result']:
            logger.info("No speedtest data available")
            return
        
        for metric_name, query in SPEED_METRICS.items():
            result = self._query_prometheus(query)
            if result and 'data' in result and 'result' in result['data']:
                for r in result['data']['result']:
                    value = float(r['value'][1])
                    # Convert bits to Mbps for speed metrics
                    if metric_name in ['download_mbps', 'upload_mbps']:
                        value = value / 1_000_000
                    metrics_data[metric_name] = value
        
        if metrics_data:
            self._insert_speed_metrics(metrics_data)

import asyncio

async def main():
    # Initialize the database
    database.init_db()
    
    monitor = NetworkMonitor()
    
    # Schedule ping metrics collection every 5 minutes
    schedule.every(5).minutes.do(monitor.collect_ping_metrics)
    
    # Schedule speedtest metrics collection every 5 minutes
    # (it will only insert data if new speedtest results are available)
    schedule.every(5).minutes.do(monitor.collect_speed_metrics)
    
    # Run initial collection
    monitor.collect_ping_metrics()
    monitor.collect_speed_metrics()
    
    # Schedule metrics upload every 24 hours or SYNC_INTERVAL
    schedule.every(monitor.sync_interval_minutes).minutes.do(lambda: asyncio.create_task(monitor.upload_metrics()))
    
    # Keep the script running
    while True:
        await asyncio.sleep(1)
        schedule.run_pending()

if __name__ == "__main__":
    asyncio.run(main())
