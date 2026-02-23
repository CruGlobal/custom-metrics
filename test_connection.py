# # Create a new virtual environment
# python3 -m venv venv
# # Activate the virtual environment
# # On macOS/Linux:
# source venv/bin/activate
# # On Windows:
# .\venv\Scripts\activate
# pip3 install -r bigquery
## to run :
# GOOGLE_APPLICATION_CREDENTIALS=./.secret.json python3 test_connection.py

from google.cloud import bigquery

BIGQUERY_PROJECT = "lmi-sheets"
BIGQUERY_DATASET = "ministry_blockers"
PING_TABLE = "Ping"
SPEED_TABLE = "Speed"

def test_db_ping_insert():
    """attempts to insert record to Google BigQuery.    """

    print("Attempting to insert record into BigQuery...")
    try:
        bigquery_client = bigquery.Client() 
        table_ref = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{PING_TABLE}"
        # Create a dummy record based on PING_TABLE_SCHEMA
        dummy_record = {
            "timestamp": "2026-02-04T14:25:00.123456",
            "site_id": "test_site",
            "location": "test_location",
            "google_up": 1,
            "apple_up": 1,
            "github_up": 1,
            "pihole_up": 1,
            "node_up": 1,
            "speedtest_up": 1,
            "http_latency": 100,
            "http_samples": 1,
            "http_time": 100,
            "http_content_length": 1000,
            "http_duration": 100
        }

        errors = bigquery_client.insert_rows_json(table_ref, [dummy_record])
        print("Successfully inserted test record into BigQuery!")
    except Exception as e:
        print(f"An error occurred during test insertion: {e}")

def test_db_speed_insert():
    """attempts to insert record to Google BigQuery for Speed table."""

    print("Attempting to insert record into BigQuery Speed table...")
    try:
        bigquery_client = bigquery.Client()
        table_ref = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{SPEED_TABLE}"
        # Create a dummy record based on common speed test metrics
        dummy_record = {
            "timestamp": "2026-02-04T14:25:00.123456",
            "site_id": "test_site",
            "location": "test_location",
            "download_mbps": 10.0,
            "upload_mbps": 10.0,
            "ping_ms": 0.3,
            "jitter_ms": 0.3,
        }

        errors = bigquery_client.insert_rows_json(table_ref, [dummy_record])
        if errors:
            print(f"Errors occurred during test insertion: {errors}")
        else:
            print("Successfully inserted test record into BigQuery Speed table!")
    except Exception as e:
        print(f"An error occurred during test insertion: {e}")

def test_read_ping():
    # Construct a SQL query to select data
    sql_query = """
SELECT
    *
FROM
    `lmi-sheets.ministry_blockers.Ping`
LIMIT 10
"""
    try:
        out = bigquery.Client().query(sql_query).to_dataframe()
        print(out.head())
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    test_read_ping()
    test_db_ping_insert()
    test_db_speed_insert()
