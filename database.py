import os
from datetime import datetime
from turso_python.connection import TursoConnection
from turso_python.crud import TursoCRUD

# Define expected types for each table's columns
PING_METRICS_SCHEMA = {
    'timestamp': str,
    'site_id': str,
    'location': str,
    'google_up': (float, type(None)),
    'apple_up': (float, type(None)),
    'github_up': (float, type(None)),
    'pihole_up': (float, type(None)),
    'node_up': (float, type(None)),
    'speedtest_up': (float, type(None)),
    'http_latency': (float, type(None)),
    'http_samples': (float, type(None)),
    'http_time': (float, type(None)),
    'http_content_length': (float, type(None)),
    'http_duration': (float, type(None)),
}

SPEED_METRICS_SCHEMA = {
    'timestamp': str,
    'site_id': str,
    'location': str,
    'download_mbps': (float, type(None)),
    'upload_mbps': (float, type(None)),
    'ping_ms': (float, type(None)),
    'jitter_ms': (float, type(None)),
}

def get_turso_crud():
    connection = TursoConnection()
    return TursoCRUD(connection)

def _validate_metrics_data(data, schema):
    """Validate if the metrics data conforms to the expected schema types."""
    for key, expected_type in schema.items():
        if key in data and not isinstance(data[key], expected_type):
            raise ValueError(f"Type mismatch for column '{key}'. Expected {expected_type}, got {type(data[key])}")

def init_db():
    """Initialize the Turso database with tables for ping and speed metrics."""
    crud = get_turso_crud()
    
    # Create ping_metrics table
    crud.connection.execute_query('''
        CREATE TABLE IF NOT EXISTS ping_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            site_id TEXT,
            location TEXT,
            google_up REAL,
            apple_up REAL,
            github_up REAL,
            pihole_up REAL,
            node_up REAL,
            speedtest_up REAL,
            http_latency REAL,
            http_samples REAL,
            http_time REAL,
            http_content_length REAL,
            http_duration REAL
        )
    ''')
    
    # Create speed_metrics table
    crud.connection.execute_query('''
        CREATE TABLE IF NOT EXISTS speed_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            site_id TEXT,
            location TEXT,
            download_mbps REAL,
            upload_mbps REAL,
            ping_ms REAL,
            jitter_ms REAL
        )
    ''')

def insert_ping_metrics(metrics_data):
    """Insert ping metrics into the Turso database."""
    crud = get_turso_crud()
    
    # Prepare the data for insertion
    data = {
        'timestamp': datetime.utcnow().isoformat(),
        'site_id': metrics_data.get('site_id'),
        'location': metrics_data.get('location'),
        'google_up': metrics_data.get('google_up'),
        'apple_up': metrics_data.get('apple_up'),
        'github_up': metrics_data.get('github_up'),
        'pihole_up': metrics_data.get('pihole_up'),
        'node_up': metrics_data.get('node_up'),
        'speedtest_up': metrics_data.get('speedtest_up'),
        'http_latency': metrics_data.get('http_latency'),
        'http_samples': metrics_data.get('http_samples'),
        'http_time': metrics_data.get('http_time'),
        'http_content_length': metrics_data.get('http_content_length'),
        'http_duration': metrics_data.get('http_duration')
    }

    _validate_metrics_data(data, PING_METRICS_SCHEMA)
    
    # Insert the data
    crud.create("ping_metrics", data)

def insert_speed_metrics(metrics_data):
    """Insert speed metrics into the Turso database."""
    crud = get_turso_crud()
    
    # Prepare the data for insertion
    data = {
        'timestamp': datetime.utcnow().isoformat(),
        'site_id': metrics_data.get('site_id'),
        'location': metrics_data.get('location'),
        'download_mbps': metrics_data.get('download_mbps'),
        'upload_mbps': metrics_data.get('upload_mbps'),
        'ping_ms': metrics_data.get('ping_ms'),
        'jitter_ms': metrics_data.get('jitter_ms')
    }

    _validate_metrics_data(data, SPEED_METRICS_SCHEMA)
    
    # Insert the data
    crud.create("speed_metrics", data)

def get_all_ping_metrics():
    """Retrieve all ping metrics from the database."""
    crud = get_turso_crud()
    return crud.read("ping_metrics")

def get_all_speed_metrics():
    """Retrieve all speed metrics from the database."""
    crud = get_turso_crud()
    return crud.read("speed_metrics")

def clear_ping_metrics():
    """Clear all ping metrics from the database."""
    crud = get_turso_crud()
    crud.connection.execute_query('DELETE FROM ping_metrics')

def clear_speed_metrics():
    """Clear all speed metrics from the database."""
    crud = get_turso_crud()
    crud.connection.execute_query('DELETE FROM speed_metrics')
