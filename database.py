import os
from datetime import datetime
from turso_python.connection import TursoConnection
from turso_python.crud import TursoCRUD

# Define expected types for each table's columns
PING_METRICS_SCHEMA = {
    'timestamp': str,
    'site_id': str,
    'location': str,
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
    'download_mbps': (str, type(None)),
    'upload_mbps': (str, type(None)),
    'ping_ms': (str, type(None)),
    'jitter_ms': (str, type(None)),
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
    ''')
    
    # Create speed_metrics table
    crud.connection.execute_query('''
        CREATE TABLE IF NOT EXISTS speed_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            site_id TEXT,
            location TEXT,
            download_mbps TEXT,
            upload_mbps TEXT,
            ping_ms TEXT,
            jitter_ms TEXT
        )
    ''')

def insert_ping_metrics(metrics_data):
    """Insert ping metrics into the Turso database."""
    crud = get_turso_crud()
    
    # Prepare the data for insertion
    data = {
        'timestamp': datetime.utcnow().isoformat(),
        'site_id': str(metrics_data.get('site_id')) if metrics_data.get('site_id') is not None else None,
        'location': str(metrics_data.get('location')) if metrics_data.get('location') is not None else None,
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
    
    # Insert the data
    crud.create("ping_metrics", data)

def insert_speed_metrics(metrics_data):
    """Insert speed metrics into the Turso database."""
    crud = get_turso_crud()
    
    # Prepare the data for insertion
    data = {
        'timestamp': datetime.utcnow().isoformat(),
        'site_id': str(metrics_data.get('site_id')) if metrics_data.get('site_id') is not None else None,
        'location': str(metrics_data.get('location')) if metrics_data.get('location') is not None else None,
        'download_mbps': str(metrics_data.get('download_mbps')) if metrics_data.get('download_mbps') is not None else None,
        'upload_mbps': str(metrics_data.get('upload_mbps')) if metrics_data.get('upload_mbps') is not None else None,
        'ping_ms': str(metrics_data.get('ping_ms')) if metrics_data.get('ping_ms') is not None else None,
        'jitter_ms': str(metrics_data.get('jitter_ms')) if metrics_data.get('jitter_ms') is not None else None
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
