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
    bigquery.SchemaField("timestamp", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("site_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("location", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("google_up", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("apple_up", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("github_up", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("pihole_up", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("node_up", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("speedtest_up", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("http_latency", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("http_samples", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("http_time", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("http_content_length", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("http_duration", "STRING", mode="NULLABLE"),
]

SPEED_TABLE_SCHEMA = [
    bigquery.SchemaField("timestamp", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("site_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("location", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("download_mbps", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("upload_mbps", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ping_ms", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("jitter_ms", "STRING", mode="NULLABLE"),
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
        # Exclude 'id' and 'synced' fields, as they are local database specific
        row = {k: v for k, v in metric.items() if k not in ['id', 'synced']}
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
            'timestamp': datetime.utcnow().isoformat(),
            'site_id': str(metric.get('site_id')) if metric.get('site_id') is not None else None,
            'location': str(metric.get('location')) if metric.get('location') is not None else None,
            'download_mbps': str(metric.get('download_mbps')) if metric.get('download_mbps') is not None else None,
            'upload_mbps': str(metric.get('upload_mbps')) if metric.get('upload_mbps') is not None else None,
            'ping_ms': str(metric.get('ping_ms')) if metric.get('ping_ms') is not None else None,
            'jitter_ms': str(metric.get('jitter_ms')) if metric.get('jitter_ms') is not None else None
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
