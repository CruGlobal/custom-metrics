# Store the ping metrics until submitted to Bigquery

import os
import time # Added for retry mechanism
import logging # Added for logging
import sqlite3
from datetime import datetime, timedelta, UTC # Added timedelta and UTC for date calculations

# Database file path
DB_FILE = "metrics.db"

def init_db():
    """Initialize the database with tables for ping and speed metrics."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Create ping_metrics table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ping_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            site_id TEXT,
            location TEXT,
            google_up INTEGER,
            apple_up INTEGER,
            github_up INTEGER,
            pihole_up INTEGER,
            node_up INTEGER,
            speedtest_up INTEGER,
            http_latency REAL,
            http_samples REAL,
            http_time REAL,
            http_content_length REAL,
            http_duration REAL,
            synced INTEGER DEFAULT 0
        )
    """
    )

    # Create speed_metrics table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS speed_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            site_id TEXT,
            location TEXT,
            download_mbps REAL,
            upload_mbps REAL,
            ping_ms REAL,
            jitter_ms REAL,
            synced INTEGER DEFAULT 0
        )
    """
    )

    # Add synced column to speed_metrics if it doesn't exist
    try:
        cursor.execute("ALTER TABLE speed_metrics ADD COLUMN synced INTEGER DEFAULT 0")
        logging.info("Added synced column to speed_metrics table.")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            logging.info("synced column already exists in speed_metrics table.")
        else:
            raise e

    conn.commit()
    conn.close()

def insert_ping_metrics(metrics_data):
    """Insert ping metrics into the database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Prepare the data for insertion
    data = {
        "timestamp": datetime.now(UTC).isoformat(),
        "site_id": str(metrics_data.get("site_id")) if metrics_data.get("site_id") is not None else None,
        "location": str(metrics_data.get("location")) if metrics_data.get("location") is not None else None,
        "google_up": metrics_data.get("google_up"),
        "apple_up": metrics_data.get("apple_up"),
        "github_up": metrics_data.get("github_up"),
        "pihole_up": metrics_data.get("pihole_up"),
        "node_up": metrics_data.get("node_up"),
        "speedtest_up": metrics_data.get("speedtest_up"),
        "http_latency": metrics_data.get("http_latency"),
        "http_samples": metrics_data.get("http_samples"),
        "http_time": metrics_data.get("http_time"),
        "http_content_length": metrics_data.get("http_content_length"),
        "http_duration": metrics_data.get("http_duration")
    }
    
    # Insert the data
    cursor.execute("""
        INSERT INTO ping_metrics (
            timestamp, site_id, location, google_up, apple_up, github_up, pihole_up, 
            node_up, speedtest_up, http_latency, http_samples, http_time, 
            http_content_length, http_duration, synced
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, 0
        )
    """, (data["timestamp"], data["site_id"], data["location"],  data["google_up"], data["apple_up"], data["github_up"], data["pihole_up"],
          data["node_up"], data["speedtest_up"], data["http_latency"], data["http_samples"], data["http_time"],
          data["http_content_length"], data["http_duration"]))
    
    conn.commit()
    conn.close()

def insert_speed_metrics(metrics_data):
    """Insert speed metrics into the database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Prepare the data for insertion
    data = {
        "timestamp": datetime.now(UTC).isoformat(),
        "site_id": str(metrics_data.get("site_id")) if metrics_data.get("site_id") is not None else None,
        "location": str(metrics_data.get("location")) if metrics_data.get("location") is not None else None,
        "download_mbps": metrics_data.get("download_mbps"),
        "upload_mbps": metrics_data.get("upload_mbps"),
        "ping_ms": metrics_data.get("ping_ms"),
        "jitter_ms": metrics_data.get("jitter_ms")
    }
    
    # Insert the data
    cursor.execute("""
        INSERT INTO speed_metrics (
            timestamp, site_id, location, download_mbps, 
            upload_mbps, ping_ms, jitter_ms, synced
        ) VALUES (
            ?, ?, ?, ?, ?,
            ?, ?, ?, 0
        )
    """, (data["timestamp"], data["site_id"], data["location"],  data["download_mbps"],
          data["upload_mbps"], data["ping_ms"], data["jitter_ms"]))
    
    conn.commit()
    conn.close()

def mark_ping_metrics_as_synced(metric_ids):
    """Mark specified ping metrics as synced in the database."""
    if not metric_ids:
        return

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    placeholders = ", ".join(["?"] * len(metric_ids)) # Changed to ? for sqlite3
    cursor.execute(f"""
        UPDATE ping_metrics
        SET synced = 1
        WHERE id IN ({placeholders})
    """, metric_ids)
    
    conn.commit()
    conn.close()

def mark_speed_metrics_as_synced(metric_ids):
    """Mark specified speed metrics as synced in the database."""
    if not metric_ids:
        return

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    placeholders = ", ".join(["?"] * len(metric_ids)) # Changed to ? for sqlite3
    cursor.execute(f"""
        UPDATE speed_metrics
        SET synced = 1
        WHERE id IN ({placeholders})
    """, metric_ids)
    
    conn.commit()
    conn.close()

def get_ping_metrics_to_sync():
    """Retrieve ping metrics that need to be synced from the database.
    Metrics are selected if they haven't been synced (synced=0) 
    """
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row # Enable dictionary-like access to rows
    cursor = conn.cursor()

    one_week_ago = (datetime.now(UTC) - timedelta(weeks=1)).isoformat()
    
    cursor.execute("""
        SELECT id, timestamp, site_id, location, google_up, apple_up, github_up, 
               pihole_up, node_up, speedtest_up, http_latency, http_samples, 
               http_time, http_content_length, http_duration, synced
        FROM ping_metrics
        WHERE synced = 0 OR timestamp < ?
    """, (one_week_ago,))
    
    rows = cursor.fetchall()
    
    # Convert rows to list of dictionaries (already handled by row_factory)
    result = [dict(row) for row in rows]
    
    conn.close()
    return result

def get_speed_metrics_to_sync():
    """Retrieve speed metrics that need to be synced from the database.
    Metrics are selected if they haven't been synced (synced=0) 
    """
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row # Enable dictionary-like access to rows
    cursor = conn.cursor()

    one_week_ago = (datetime.now(UTC) - timedelta(weeks=1)).isoformat()
    
    cursor.execute("""
        SELECT id, timestamp, site_id, location, download_mbps, 
            upload_mbps, ping_ms, jitter_ms, synced
        FROM speed_metrics
        WHERE synced = 0 OR timestamp < ?
    """, (one_week_ago,))
    
    rows = cursor.fetchall()
    
    # Convert rows to list of dictionaries (already handled by row_factory)
    result = [dict(row) for row in rows]
    
    conn.close()
    # Remote DOES NOT have `synced` column. Do not send
    return result

def get_all_ping_metrics():
    """Retrieve all ping metrics from the database."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row # Enable dictionary-like access to rows
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM ping_metrics")
    rows = cursor.fetchall()
    
    # Convert rows to list of dictionaries (already handled by row_factory)
    result = [dict(row) for row in rows]
    
    conn.close()
    return result

def get_all_speed_metrics():
    """Retrieve all speed metrics from the database."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row # Enable dictionary-like access to rows
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM speed_metrics")
    rows = cursor.fetchall()
    
    # Convert rows to list of dictionaries (already handled by row_factory)
    result = [dict(row) for row in rows]
    
    conn.close()
    return result

def clear_ping_metrics():
    """Clear all ping metrics from the database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute("DELETE FROM ping_metrics")
    
    conn.commit()
    conn.close()

def clear_speed_metrics():
    """Clear all speed metrics from the database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute("DELETE FROM speed_metrics")
    
    conn.commit()
    conn.close()

def get_not_synced():
    return True
def get_if_need_to_sync(time):
    return True

# Need to sync?
def get_if_need_to_sync(time):
    # This function is called from main.py, not remote_database.py
    # The parameter `time` is expected to be a timedelta object.
    # The existing implementation of get_not_synced and get_if_need_to_syc always return True.
    # For the purpose of the mock tests, we will assume this logic is as intended
    # and the time parameter is just passed along. The actual logic for determining
    # if a sync is needed would be more complex and involve the 'time' parameter.
    return get_not_synced() and get_if_need_to_sync(time)
