import sqlite3
import os
from datetime import datetime

# Database file path
DB_FILE = "metrics.db"

def init_db():
    """Initialize the SQLite database with tables for ping and speed metrics."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Create ping_metrics table
    cursor.execute('''
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
    cursor.execute('''
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
    
    conn.commit()
    conn.close()

def insert_ping_metrics(metrics_data):
    """Insert ping metrics into the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
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
    
    # Insert the data
    cursor.execute('''
        INSERT INTO ping_metrics (
            timestamp, site_id, location, google_up, apple_up, github_up, pihole_up, 
            node_up, speedtest_up, http_latency, http_samples, http_time, 
            http_content_length, http_duration
        ) VALUES (
            :timestamp, :site_id, :location, :google_up, :apple_up, :github_up, :pihole_up,
            :node_up, :speedtest_up, :http_latency, :http_samples, :http_time,
            :http_content_length, :http_duration
        )
    ''', data)
    
    conn.commit()
    conn.close()

def insert_speed_metrics(metrics_data):
    """Insert speed metrics into the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
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
    
    # Insert the data
    cursor.execute('''
        INSERT INTO speed_metrics (
            timestamp, site_id, location, download_mbps, upload_mbps, ping_ms, jitter_ms
        ) VALUES (
            :timestamp, :site_id, :location, :download_mbps, :upload_mbps, :ping_ms, :jitter_ms
        )
    ''', data)
    
    conn.commit()
    conn.close()

def get_all_ping_metrics():
    """Retrieve all ping metrics from the database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM ping_metrics')
    rows = cursor.fetchall()
    
    # Get column names
    column_names = [description[0] for description in cursor.description]
    
    # Convert rows to list of dictionaries
    result = [dict(zip(column_names, row)) for row in rows]
    
    conn.close()
    return result

def get_all_speed_metrics():
    """Retrieve all speed metrics from the database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM speed_metrics')
    rows = cursor.fetchall()
    
    # Get column names
    column_names = [description[0] for description in cursor.description]
    
    # Convert rows to list of dictionaries
    result = [dict(zip(column_names, row)) for row in rows]
    
    conn.close()
    return result

def clear_ping_metrics():
    """Clear all ping metrics from the database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('DELETE FROM ping_metrics')
    
    conn.commit()
    conn.close()

def clear_speed_metrics():
    """Clear all speed metrics from the database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('DELETE FROM speed_metrics')
    
    conn.commit()
    conn.close()
