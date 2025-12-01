import os
import time # Added for retry mechanism
import logging # Added for logging
from datetime import datetime
import psycopg

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database retry configuration
MAX_DB_RETRIES = 10
DB_RETRY_DELAY_SECONDS = 10

LOCAL_DB_PATH = "/data/metrics.db"
# The connection string is now handled by environment variables

# Define expected types for each table's columns
PING_METRICS_SCHEMA = {
    'timestamp': str,
    'site_id': str,
    'location': str,
    'ip_address': (str, type(None)),
    'google_up': (str, type(None)),
    'apple_up': (str, type(None)),
    'github_up': (str, type(None)),
    'pihole_up': (str, type(None)),
    'node_up': (str, type(None)),
    'speedtest_up': (str, type(None)),
    'http_latency': (str, type(None)),
    'http_samples': (str, type(None)),
    'http_time': (str, type(None)),
    'http_content_length': (str, type(None)),
    'http_duration': (str, type(None)),
}

SPEED_METRICS_SCHEMA = {
    'timestamp': str,
    'site_id': str,
    'location': str,
    'ip_address': (str, type(None)),
    'download_mbps': (str, type(None)),
    'upload_mbps': (str, type(None)),
    'ping_ms': (str, type(None)),
    'jitter_ms': (str, type(None)),
}

def get_db_connection():
    # Construct connection string explicitly from environment variables
    conn_string = (
        f"host={os.getenv('PGHOST')} "
        f"user={os.getenv('PGUSER')} "
        f"password={os.getenv('PGPASSWORD')} "
        f"dbname={os.getenv('PGDATABASE')} "
        f"sslmode={os.getenv('PGSSLMODE')} "
        f"channel_binding={os.getenv('PGCHANNELBINDING')} connect_timeout=500"
    )
    return psycopg.connect(conn_string)

def _validate_metrics_data(data, schema):
    """Validate if the metrics data conforms to the expected schema types."""
    for key, expected_type in schema.items():
        if key in data and not isinstance(data[key], expected_type):
            raise ValueError(f"Type mismatch for column '{key}'. Expected {expected_type}, got {type(data[key])}")

def init_db():
    """Initialize the database with tables for ping and speed metrics with retry mechanism."""
    for i in range(1, MAX_DB_RETRIES + 1):
        try:
            logger.info(f"Attempting to initialize database (Attempt {i}/{MAX_DB_RETRIES})...")
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ping_metrics (
                    id SERIAL PRIMARY KEY,
                    timestamp TEXT,
                    site_id TEXT,
                    location TEXT,
                    ip_address TEXT,
                    google_up TEXT,
                    apple_up TEXT,
                    github_up TEXT,
                    pihole_up TEXT,
                    node_up TEXT,
                    speedtest_up TEXT,
                    http_latency TEXT,
                    http_samples TEXT,
                    http_time TEXT,
                    http_content_length TEXT,
                    http_duration TEXT
                )
            """
            )
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS speed_metrics (
                    id SERIAL PRIMARY KEY,
                    timestamp TEXT,
                    site_id TEXT,
                    location TEXT,
                    ip_address TEXT,
                    download_mbps TEXT,
                    upload_mbps TEXT,
                    ping_ms TEXT,
                    jitter_ms TEXT
                )
            """
            )
            conn.commit()
            conn.close()
            logger.info("Database initialized successfully.")
            break # Exit loop if connection is successful
        except psycopg.OperationalError as e:
            logger.error(f"Database connection failed: {e}")
            if i < MAX_DB_RETRIES:
                logger.info(f"Retrying database connection in {DB_RETRY_DELAY_SECONDS} seconds...")
                time.sleep(DB_RETRY_DELAY_SECONDS)
            else:
                logger.critical("Maximum database connection retries reached. Exiting.")
                raise e
        except Exception as e:
            logger.critical(f"An unexpected error occurred during database initialization: {e}. Exiting.")
            raise e

def insert_ping_metrics(metrics_data):
    """Insert ping metrics into the database."""
    data = {
        'timestamp': datetime.utcnow().isoformat(),
        'site_id': str(metrics_data.get('site_id')) if metrics_data.get('site_id') is not None else None,
        'location': str(metrics_data.get('location')) if metrics_data.get('location') is not None else None,
        'ip_address': str(metrics_data.get('ip_address')) if metrics_data.get('ip_address') is not None else None,
        'google_up': str(metrics_data.get('google_up')) if metrics_data.get('google_up') is not None else None,
        'apple_up': str(metrics_data.get('apple_up')) if metrics_data.get('apple_up') is not None else None,
        'github_up': str(metrics_data.get('github_up')) if metrics_data.get('github_up') is not None else None,
        'pihole_up': str(metrics_data.get('pihole_up')) if metrics_data.get('pihole_up') is not None else None,
        'node_up': str(metrics_data.get('node_up')) if metrics_data.get('node_up') is not None else None,
        'speedtest_up': str(metrics_data.get('speedtest_up')) if metrics_data.get('speedtest_up') is not None else None,
        'http_latency': str(metrics_data.get('http_latency')) if metrics_data.get('http_latency') is not None else None,
        'http_samples': str(metrics_data.get('http_samples')) if metrics_data.get('http_samples') is not None else None,
        'http_time': str(metrics_data.get('http_time')) if metrics_data.get('http_time') is not None else None,
        'http_content_length': str(metrics_data.get('http_content_length')) if metrics_data.get('http_content_length') is not None else None,
        'http_duration': str(metrics_data.get('http_duration')) if metrics_data.get('http_duration') is not None else None
    }

    _validate_metrics_data(data, PING_METRICS_SCHEMA)

    conn = get_db_connection()
    cursor = conn.cursor()
    columns = ", ".join(data.keys())
    placeholders = ", ".join(["%s"] * len(data))
    cursor.execute(f"INSERT INTO ping_metrics ({columns}) VALUES ({placeholders})", list(data.values()))
    conn.commit()
    conn.close()

def insert_speed_metrics(metrics_data):
    """Insert speed metrics into the database."""
    data = {
        'timestamp': datetime.utcnow().isoformat(),
        'site_id': str(metrics_data.get('site_id')) if metrics_data.get('site_id') is not None else None,
        'location': str(metrics_data.get('location')) if metrics_data.get('location') is not None else None,
        'ip_address': str(metrics_data.get('ip_address')) if metrics_data.get('ip_address') is not None else None,
        'download_mbps': str(metrics_data.get('download_mbps')) if metrics_data.get('download_mbps') is not None else None,
        'upload_mbps': str(metrics_data.get('upload_mbps')) if metrics_data.get('upload_mbps') is not None else None,
        'ping_ms': str(metrics_data.get('ping_ms')) if metrics_data.get('ping_ms') is not None else None,
        'jitter_ms': str(metrics_data.get('jitter_ms')) if metrics_data.get('jitter_ms') is not None else None
    }

    _validate_metrics_data(data, SPEED_METRICS_SCHEMA)

    conn = get_db_connection()
    cursor = conn.cursor()
    columns = ", ".join(data.keys())
    placeholders = ", ".join(["%s"] * len(data))
    cursor.execute(f"INSERT INTO speed_metrics ({columns}) VALUES ({placeholders})", list(data.values()))
    conn.commit()
    conn.close()

def get_all_ping_metrics():
    """Retrieve all ping metrics from the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM ping_metrics")
    metrics = cursor.fetchall()
    conn.close()
    return metrics

def get_all_speed_metrics():
    """Retrieve all speed metrics from the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM speed_metrics")
    metrics = cursor.fetchall()
    conn.close()
    return metrics

def clear_ping_metrics():
    """Clear all ping metrics from the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM ping_metrics")
    conn.commit()
    conn.close()

def clear_speed_metrics():
    """Clear all speed metrics from the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM speed_metrics")
    conn.commit()
    conn.close()
