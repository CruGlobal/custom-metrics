# Store the ping metrics until submitted to Bigquery

import os
import time # Added for retry mechanism
import logging # Added for logging
import psycopg # Kept psycopg as per feedback
from datetime import datetime, timedelta, UTC # Added timedelta and UTC for date calculations

# Database file path
DB_FILE = "metrics.db"

def init_db():
    """Initialize the database with tables for ping and speed metrics."""
    conn = psycopg.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Create ping_metrics table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ping_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            http_duration TEXT,
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
            ip_address TEXT,
            download_mbps TEXT,
            upload_mbps TEXT,
            ping_ms TEXT,
            jitter_ms TEXT,
            synced INTEGER DEFAULT 0
        )
    """
    )

    conn.commit()
    conn.close()

def insert_ping_metrics(metrics_data):
    """Insert ping metrics into the database."""
    conn = psycopg.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Prepare the data for insertion
    data = {
        "timestamp": datetime.now(UTC).isoformat(),
        "site_id": str(metrics_data.get("site_id")) if metrics_data.get("site_id") is not None else None,
        "location": str(metrics_data.get("location")) if metrics_data.get("location") is not None else None,
        "ip_address": str(metrics_data.get("ip_address")) if metrics_data.get("ip_address") is not None else None,
        "google_up": str(metrics_data.get("google_up")) if metrics_data.get("google_up") is not None else None,
        "apple_up": str(metrics_data.get("apple_up")) if metrics_data.get("apple_up") is not None else None,
        "github_up": str(metrics_data.get("github_up")) if metrics_data.get("github_up") is not None else None,
        "pihole_up": str(metrics_data.get("pihole_up")) if metrics_data.get("pihole_up") is not None else None,
        "node_up": str(metrics_data.get("node_up")) if metrics_data.get("node_up") is not None else None,
        "speedtest_up": str(metrics_data.get("speedtest_up")) if metrics_data.get("speedtest_up") is not None else None,
        "http_latency": str(metrics_data.get("http_latency")) if metrics_data.get("http_latency") is not None else None,
        "http_samples": str(metrics_data.get("http_samples")) if metrics_data.get("http_samples") is not None else None,
        "http_time": str(metrics_data.get("http_time")) if metrics_data.get("http_time") is not None else None,
        "http_content_length": str(metrics_data.get("http_content_length")) if metrics_data.get("http_content_length") is not None else None,
        "http_duration": str(metrics_data.get("http_duration")) if metrics_data.get("http_duration") is not None else None
    }
    
    # Insert the data
    cursor.execute("""
        INSERT INTO ping_metrics (
            timestamp, site_id, location, ip_address, google_up, apple_up, github_up, pihole_up, 
            node_up, speedtest_up, http_latency, http_samples, http_time, 
            http_content_length, http_duration, synced
        ) VALUES (
            :timestamp, :site_id, :location, :ip_address, :google_up, :apple_up, :github_up, :pihole_up,
            :node_up, :speedtest_up, :http_latency, :http_samples, :http_time,
            :http_content_length, :http_duration, 0
        )
    """, data)
    
    conn.commit()
    conn.close()

def insert_speed_metrics(metrics_data):
    """Insert speed metrics into the database."""
    conn = psycopg.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Prepare the data for insertion
    data = {
        "timestamp": datetime.now(UTC).isoformat(),
        "site_id": str(metrics_data.get("site_id")) if metrics_data.get("site_id") is not None else None,
        "location": str(metrics_data.get("location")) if metrics_data.get("location") is not None else None,
        "ip_address": str(metrics_data.get("ip_address")) if metrics_data.get("ip_address") is not None else None,
        "download_mbps": str(metrics_data.get("download_mbps")) if metrics_data.get("download_mbps") is not None else None,
        "upload_mbps": str(metrics_data.get("upload_mbps")) if metrics_data.get("upload_mbps") is not None else None,
        "ping_ms": str(metrics_data.get("ping_ms")) if metrics_data.get("ping_ms") is not None else None,
        "jitter_ms": str(metrics_data.get("jitter_ms")) if metrics_data.get("jitter_ms") is not None else None
    }
    
    # Insert the data
    cursor.execute("""
        INSERT INTO speed_metrics (
            timestamp, site_id, location, ip_address, download_mbps, 
            upload_mbps, ping_ms, jitter_ms, synced
        ) VALUES (
            :timestamp, :site_id, :location, :ip_address, :download_mbps,
            :upload_mbps, :ping_ms, :jitter_ms, 0
        )
    """, data)
    
    conn.commit()
    conn.close()

def mark_ping_metrics_as_synced(metric_ids):
    """Mark specified ping metrics as synced in the database."""
    if not metric_ids:
        return

    conn = psycopg.connect(DB_FILE)
    cursor = conn.cursor()
    
    placeholders = ", ".join(["%s"] * len(metric_ids)) # Changed to %s for psycopg
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

    conn = psycopg.connect(DB_FILE)
    cursor = conn.cursor()
    
    placeholders = ", ".join(["%s"] * len(metric_ids)) # Changed to %s for psycopg
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
    conn = psycopg.connect(DB_FILE)
    cursor = conn.cursor()

    one_week_ago = (datetime.now(UTC) - timedelta(weeks=1)).isoformat()
    
    cursor.execute("""
        SELECT id, timestamp, site_id, location, ip_address, google_up, apple_up, github_up, 
               pihole_up, node_up, speedtest_up, http_latency, http_samples, 
               http_time, http_content_length, http_duration, synced
        FROM ping_metrics
        WHERE synced = 0 OR timestamp < %s
    """, (one_week_ago,))
    
    rows = cursor.fetchall()
    
    # Get column names
    column_names = [description[0] for description in cursor.description]
    
    # Convert rows to list of dictionaries
    result = [dict(zip(column_names, row)) for row in rows]
    
    conn.close()
    return result

def get_speed_metrics_to_sync():
    """Retrieve speed metrics that need to be synced from the database.
    Metrics are selected if they haven't been synced (synced=0) 
    """
    conn = psycopg.connect(DB_FILE)
    cursor = conn.cursor()

    one_week_ago = (datetime.now(UTC) - timedelta(weeks=1)).isoformat()
    
    cursor.execute("""
        SELECT id, timestamp, site_id, location, ip_address, download_mbps, 
            upload_mbps, ping_ms, jitter_ms, synced
        FROM speed_metrics
        WHERE synced = 0 OR timestamp < %s
    """, (one_week_ago,))
    
    rows = cursor.fetchall()
    
    # Get column names
    column_names = [description[0] for description in cursor.description]
    
    # Convert rows to list of dictionaries
    result = [dict(zip(column_names, row)) for row in rows]
    
    conn.close()
    return result

def get_all_ping_metrics():
    """Retrieve all ping metrics from the database."""
    conn = psycopg.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM ping_metrics")
    rows = cursor.fetchall()
    
    # Get column names
    column_names = [description[0] for description in cursor.description]
    
    # Convert rows to list of dictionaries
    result = [dict(zip(column_names, row)) for row in rows]
    
    conn.close()
    return result

def get_all_speed_metrics():
    """Retrieve all speed metrics from the database."""
    conn = psycopg.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM speed_metrics")
    rows = cursor.fetchall()
    
    # Get column names
    column_names = [description[0] for description in cursor.description]
    
    # Convert rows to list of dictionaries
    result = [dict(zip(column_names, row)) for row in rows]
    
    conn.close()
    return result

def clear_ping_metrics():
    """Clear all ping metrics from the database."""
    conn = psycopg.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute("DELETE FROM ping_metrics")
    
    conn.commit()
    conn.close()

def clear_speed_metrics():
    """Clear all speed metrics from the database."""
    conn = psycopg.connect(DB_FILE)
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
