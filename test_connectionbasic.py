# this is useful for standardized debugging
from google.cloud import bigquery

# SITE_ID_FILE = "/etc/network-monitor/site_id"
# BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT", "your-project-id")
# BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "internet_monitoring1")
# def _init_bigquery_client(self):
#     """Initialize BigQuery client with service account credentials."""
#     try: 
#         return bigquery.Client()
#     except Exception as e:
#         print.error(f"Failed to initialize BigQuery client: {e}")
#         raise

def test_db_connection():
    """
    Tests the connection to Google BigQuery.
    """

    print("Attempting to connect to BigQuery...")
    try:
        # Construct a BigQuery client object.
        client = bigquery.Client()

        # Perform a simple query to test the connection
        query_job = client.query("SELECT 1;")
        result = query_job.result() # Waits for job to complete.

        print("BigQuery connection successful!")
        for row in result:
            print(f"Test query result: {row[0]}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # bigquery_client = _init_bigquery_client()
    test_db_connection()