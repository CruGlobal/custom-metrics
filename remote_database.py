import os
import time
import logging
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from google.oauth2 import service_account
import local_database

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database retry configuration
MAX_DB_RETRIES = 10
DB_RETRY_DELAY_SECONDS = 10

PROJECT_ID = os.getenv('PROJECT_ID')
DATASET_ID = os.getenv('DATASET_ID')
PING_TABLE_ID = os.getenv('PING_TABLE_ID')
SPEED_TABLE_ID = os.getenv('SPEED_TABLE_ID')

# BigQuery client instance
client = None

def get_bigquery_client():
    global client
    if client is None:
        try:
            client = bigquery.Client()
            return client
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            raise

# Define BigQuery schemas
PING_TABLE_SCHEMA = [
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="NULLABLE"), # TIMESTAMP
    bigquery.SchemaField("site_id", "STRING", mode="NULLABLE"), # STRING
    bigquery.SchemaField("location", "STRING", mode="NULLABLE"), # STRING
    bigquery.SchemaField("google_up", "NUMERIC", mode="NULLABLE"), # NUMERIC
    bigquery.SchemaField("apple_up", "NUMERIC", mode="NULLABLE"), # NUMERIC
    bigquery.SchemaField("github_up", "NUMERIC", mode="NULLABLE"), # NUMERIC
    bigquery.SchemaField("pihole_up", "NUMERIC", mode="NULLABLE"),# NUMERIC
    bigquery.SchemaField("node_up", "NUMERIC", mode="NULLABLE"),# NUMERIC
    bigquery.SchemaField("speedtest_up", "NUMERIC", mode="NULLABLE"),# NUMERIC
    bigquery.SchemaField("http_latency", "NUMERIC", mode="NULLABLE"),# NUMERIC
    bigquery.SchemaField("http_samples", "NUMERIC", mode="NULLABLE"),# NUMERIC
    bigquery.SchemaField("http_time", "NUMERIC", mode="NULLABLE"),# NUMERIC
    bigquery.SchemaField("http_content_length", "NUMERIC", mode="NULLABLE"),# NUMERIC
    bigquery.SchemaField("http_duration", "NUMERIC", mode="NULLABLE"),# NUMERIC
]

SPEED_TABLE_SCHEMA = [
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="NULLABLE"), # TIMESTAMP
    bigquery.SchemaField("site_id", "STRING", mode="NULLABLE"), # STRING
    bigquery.SchemaField("location", "STRING", mode="NULLABLE"), # STRING
    bigquery.SchemaField("download_mbps", "NUMERIC", mode="NULLABLE"),# NUMERIC
    bigquery.SchemaField("upload_mbps", "NUMERIC", mode="NULLABLE"),# NUMERIC
    bigquery.SchemaField("ping_ms", "NUMERIC", mode="NULLABLE"),# NUMERIC
    bigquery.SchemaField("jitter_ms", "NUMERIC", mode="NULLABLE"),# NUMERIC
]

def init_db():
    """Initialize the BigQuery dataset and tables with retry mechanism."""
    bqc = get_bigquery_client()
    dataset_ref = bqc.dataset(DATASET_ID)

    for i in range(1, MAX_DB_RETRIES + 1):
        try:
            logger.info(f"Attempting to initialize BigQuery (Attempt {i}/{MAX_DB_RETRIES})...")

            # Create dataset if it doesn't exist
            try:
                bqc.get_dataset(dataset_ref)
                logger.info(f"Dataset '{DATASET_ID}' already exists.")
            except NotFound:
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"  # Set the appropriate location
                bqc.create_dataset(dataset)
                logger.info(f"Dataset '{DATASET_ID}' created.")

            # Create ping_metrics table
            ping_table_ref = dataset_ref.table(PING_TABLE_ID)
            try:
                bqc.get_table(ping_table_ref)
                logger.info(f"Table '{PING_TABLE_ID}' already exists.")
            except NotFound:
                table = bigquery.Table(ping_table_ref, schema=PING_TABLE_SCHEMA)
                bqc.create_table(table)
                logger.info(f"Table '{PING_TABLE_ID}' created.")

            # Create speed_metrics table
            speed_table_ref = dataset_ref.table(SPEED_TABLE_ID)
            try:
                bqc.get_table(speed_table_ref)
                logger.info(f"Table '{SPEED_TABLE_ID}' already exists.")
            except NotFound:
                table = bigquery.Table(speed_table_ref, schema=SPEED_TABLE_SCHEMA)
                bqc.create_table(table)
                logger.info(f"Table '{SPEED_TABLE_ID}' created.")

            logger.info("BigQuery initialized successfully.")
            break
        except Exception as e:
            logger.error(f"BigQuery initialization failed: {e}")
            if i < MAX_DB_RETRIES:
                logger.info(f"Retrying BigQuery initialization in {DB_RETRY_DELAY_SECONDS} seconds...")
                time.sleep(DB_RETRY_DELAY_SECONDS)
            else:
                logger.critical("Maximum BigQuery initialization retries reached. Exiting.")
                raise e

def insert_ping_metrics():
    """Insert ping metrics from local into BigQuery."""
    metrics_to_sync = local_database.get_ping_metrics_to_sync()

    if not metrics_to_sync:
        logger.info("No ping metrics to sync.")
        return

    bqc = get_bigquery_client()
    table_ref = bqc.dataset(DATASET_ID).table(PING_TABLE_ID)

    rows_to_insert = []
    metric_ids_to_mark_synced = []

    for metric in metrics_to_sync:
        row = {
            "timestamp": datetime.fromisoformat(metric["timestamp"]) if metric.get("timestamp") else None,
            "site_id": metric.get("site_id"),
            "location": metric.get("location"),
            "google_up": metric.get("google_up"),
            "apple_up": metric.get("apple_up"),
            "github_up": metric.get("github_up"),
            "pihole_up": metric.get("pihole_up"),
            "node_up": metric.get("node_up"),
            "speedtest_up": metric.get("speedtest_up"),
            "http_latency": metric.get("http_latency"),
            "http_samples": metric.get("http_samples"),
            "http_time": metric.get("http_time"),
            "http_content_length": metric.get("http_content_length"),
            "http_duration": metric.get("http_duration"),
        }
        rows_to_insert.append(row)
        metric_ids_to_mark_synced.append(metric['id'])

    if rows_to_insert:
        errors = bqc.insert_rows_json(table_ref, rows_to_insert)
        if errors:
            logger.error(f"Errors encountered while inserting ping metrics: {errors}")
        else:
            local_database.mark_ping_metrics_as_synced(metric_ids_to_mark_synced)
            logger.info(f"Successfully inserted {len(rows_to_insert)} ping metrics into BigQuery and marked as synced.")

def insert_speed_metrics(metrics_data):
    """Insert speed metrics into BigQuery."""
    bqc = get_bigquery_client()
    table_ref = bqc.dataset(DATASET_ID).table(SPEED_TABLE_ID)

    rows_to_insert = []
    for metric in metrics_data:
        row = {
            'timestamp': datetime.fromisoformat(metric["timestamp"]) if metric.get("timestamp") else None,
            'site_id': metric.get('site_id'),
            'location': metric.get('location'),
            'download_mbps': metric.get('download_mbps'),
            'upload_mbps': metric.get('upload_mbps'),
            'ping_ms': metric.get('ping_ms'),
            'jitter_ms': metric.get('jitter_ms')
        }
        rows_to_insert.append(row)

    if rows_to_insert:
        errors = bqc.insert_rows_json(table_ref, rows_to_insert)
        if errors:
            logger.error(f"Errors encountered while inserting speed metrics: {errors}")
        else:
            logger.info(f"Successfully inserted {len(rows_to_insert)} speed metrics into BigQuery.")

def get_all_ping_metrics():
    """Retrieve all ping metrics from BigQuery."""
    bqc = get_bigquery_client()
    query_job = bqc.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{PING_TABLE_ID}`")
    rows = query_job.result()
    return [dict(row) for row in rows]

def get_all_speed_metrics():
    """Retrieve all speed metrics from BigQuery."""
    bqc = get_bigquery_client()
    query_job = bqc.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{SPEED_TABLE_ID}`")
    rows = query_job.result()
    return [dict(row) for row in rows]

def clear_ping_metrics():
    """Clear all ping metrics from BigQuery."""
    bqc = get_bigquery_client()
    query_job = bqc.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{PING_TABLE_ID}` WHERE TRUE")
    query_job.result()
    logger.info("All ping metrics cleared from BigQuery.")

def clear_speed_metrics():
    """Clear all speed metrics from BigQuery."""
    bqc = get_bigquery_client()
    query_job = bqc.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{SPEED_TABLE_ID}` WHERE TRUE")
    query_job.result()
    logger.info("All speed metrics cleared from BigQuery.")
