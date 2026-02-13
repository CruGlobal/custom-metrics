# Store the ping metrics until submitted to Bigquery

import os
import time # Added for retry mechanism
import logging # Added for logging
import sqlite3
from datetime import datetime, timedelta, UTC 

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
            http_content_length, http_duration
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?
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
    
    cursor.execute("""
        SELECT id, timestamp, site_id, location, google_up, apple_up, github_up, 
               pihole_up, node_up, speedtest_up, http_latency, http_samples, 
               http_time, http_content_length, http_duration, synced
        FROM ping_metrics
        WHERE synced = 0 
    """)
    
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
    
    cursor.execute("""
        SELECT id, timestamp, site_id, location, download_mbps, 
            upload_mbps, ping_ms, jitter_ms, synced
        FROM speed_metrics
        WHERE synced = 0
    """ )
    
    rows = cursor.fetchall()
    
    # Convert rows to list of dictionaries (already handled by row_factory)
    result = [dict(row) for row in rows]
    
    conn.close()
    # Remote DOES NOT have `synced` column. Do not send
    return result

def get_never_synced():
    """
    Return True if:
    1. Records exist in speed_metrics table.
    AND
    2. No records have ever been successfully synced ( = 1).
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM speed_metrics")
    total_records = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM speed_metrics WHERE synced = 1")
    synced_records = cursor.fetchone()[0]

    conn.close()

    # logging.info(f"get_never_synced() total_records {total_records}  synced_records {synced_records} ")
    return total_records > 0 and synced_records == 0

def get_if_need_to_sync():
    """
    Checks if speed metrics need to be synced.
    Returns True if:
    1. No records have ever been successfully synced (get_never_synced()).
    OR
    2. The last 'synced' record in speed_metrics was more than a week ago from NOW.
    """
    if get_never_synced():
        # logging.info(f"get_if_need_to_sync(): get_never_synced() TRUE")
        return True

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(timestamp) FROM speed_metrics WHERE synced = 1")
    last_synced_timestamp_str = cursor.fetchone()[0]
    conn.close()

    if last_synced_timestamp_str:
        last_synced_datetime = datetime.fromisoformat(last_synced_timestamp_str)
        one_week_ago = datetime.now(UTC) - timedelta(weeks=1)
        # logging.info(f"get_if_need_to_sync(): last_synced_timestamp_str {last_synced_datetime} one_week_ago {one_week_ago}")
        return last_synced_datetime < one_week_ago
    else:
        # logging.info(f"get_if_need_to_sync(): default False")
        return False
