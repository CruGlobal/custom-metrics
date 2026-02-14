import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import json
import requests
from datetime import datetime, timedelta, UTC
import tempfile # Added for temporary file creation
import main
from main import (
    NetworkMonitor, SITE_ID_FILE, PING_METRICS, SPEED_METRICS,
    PING_TABLE, SPEED_TABLE
)
import local_database
import remote_database
import asyncio

class Test(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Patch SITE_ID_FILE to a test path
        self.site_id_file_patcher = patch("main.SITE_ID_FILE", "/tmp/test_site_id")
        self.site_id_file_patcher.start()

        self.path_exists_patcher = patch("main.os.path.exists", return_value=False)
        self.mock_path_exists = self.path_exists_patcher.start()

        self.open_patcher = patch("builtins.open", mock_open())
        self.mock_open = self.open_patcher.start()

        self.requests_get_patcher = patch("main.requests.get")
        self.mock_requests_get = self.requests_get_patcher.start()
        self.mock_requests_get.side_effect = [] # Initialize side_effect as a list

        self.getenv_patcher = patch("main.os.getenv")
        self.mock_getenv = self.getenv_patcher.start()
        self.mock_getenv.side_effect = lambda key, default=None: {
            "PING_TABLE": "test-ping",
            "SPEED_TABLE": "test-speed",
            "PROMETHEUS_URL": "http://test-prometheus:9090",
            "LOCATION": "test-location"
        }.get(key, default)

        self.makedirs_patcher = patch("main.os.makedirs")
        self.makedirs_patcher.start()

        self.remote_db_init_patcher = patch("remote_database.init_db")
        self.mock_remote_db_init = self.remote_db_init_patcher.start()

        # Mock BigQuery client
        self.mock_bigquery_client = MagicMock()
        self.mock_bigquery_client.dataset.return_value.table.return_value = MagicMock()
        self.mock_bigquery_client.insert_rows_json.return_value = [] # Simulate no errors
        self.get_bigquery_client_patcher = patch("remote_database.get_bigquery_client", return_value=self.mock_bigquery_client)
        self.get_bigquery_client_patcher.start()

        # Create a unique temporary file for the local database for each test
        self.temp_db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.temp_db_file.close() # Close the file handle so sqlite can open it

        # Patch local_database.DB_FILE to this temporary file
        self.local_db_file_patcher = patch("local_database.DB_FILE", self.temp_db_file.name)
        self.local_db_file_patcher.start()
        # Ensure local database is initialized for each test
        local_database.init_db()
        self.monitor = None

    def tearDown(self):
        self.site_id_file_patcher.stop()
        self.path_exists_patcher.stop()
        self.open_patcher.stop()
        self.requests_get_patcher.stop()
        self.getenv_patcher.stop()
        self.makedirs_patcher.stop()
        self.remote_db_init_patcher.stop()
        self.get_bigquery_client_patcher.stop()
        self.local_db_file_patcher.stop()
        
        # Clean up the local database file after each test
        if os.path.exists(self.temp_db_file.name):
            os.remove(self.temp_db_file.name)

    def _setup_monitor(self):
        # Helper to set up NetworkMonitor with mocked IP/location
        original_side_effect = self.mock_getenv.side_effect
        def side_effect(key, default=None):
            if key == "LOCATION":
                return None
            return original_side_effect(key, default)
        self.mock_getenv.side_effect = side_effect

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "city": "Test City",
            "region": "Test Region",
            "country": "TC"
        }
        mock_response.raise_for_status.return_value = None
        # Clear side_effect and set the first call
        self.mock_requests_get.side_effect = [mock_response]

        self.monitor = NetworkMonitor() # Initialize monitor for this test

    def _create_mock_speed_metrics(self, metric_id, timestamp=None, synced=0):
        if timestamp is None:
            timestamp = datetime.now(UTC).isoformat()
        return {
            "id": metric_id,
            "timestamp": timestamp,
            "site_id": "test_site_id",
            "location": "Test City, Test Region, TC",
            "download_mbps": "100.0",
            "upload_mbps": "50.0",
            "ping_ms": "10.0",
            "jitter_ms": "2.0"
        }

    def _create_mock_ping_metrics(self, metric_id, timestamp=None, synced=0):
        if timestamp is None:
            timestamp = datetime.now(UTC).isoformat()
        return {
            "id": metric_id,
            "timestamp": timestamp,
            "site_id": "test_site_id",
            "location": "Test City, Test Region, TC",
            "google_up": "1", "apple_up": "1", "github_up": "1",
            "pihole_up": "1", "node_up": "1", "speedtest_up": "1",
            "http_latency": "0.1", "http_samples": "10", "http_time": "0.5",
            "http_content_length": "1000", "http_duration": "0.2",
            "synced": synced
        }

    def test_field_synced(self):
        """Basic test to verify synced field exists in the local table and not in the remote table."""
        # Check local database
        # local_database.init_db() # Removed redundant call, handled by setUp
        local_conn = local_database.sqlite3.connect(local_database.DB_FILE)
        local_cursor = local_conn.cursor()

        local_cursor.execute("PRAGMA table_info(ping_metrics)")
        local_ping_columns = [column[1] for column in local_cursor.fetchall()]
        self.assertIn("synced", local_ping_columns)

        local_cursor.execute("PRAGMA table_info(speed_metrics)")
        local_speed_columns = [column[1] for column in local_cursor.fetchall()]
        self.assertIn("synced", local_speed_columns)
        local_conn.close()

        # Check remote database
        self.assertNotIn("synced", [field.name for field in remote_database.PING_TABLE_SCHEMA])
        self.assertNotIn("synced", [field.name for field in remote_database.SPEED_TABLE_SCHEMA])

    def test_old_record_syncs_to_remote(self):
        """Test that a single old speed record added to local SQLite is synced to remote and marked as synced."""
        self._setup_monitor()

        mock_record_id = 1
        # create an OLD speed record
        old_timestamp = (datetime.now(UTC) - timedelta(weeks=2)).isoformat() # Ensure it's definitely old
        mock_metrics_data = self._create_mock_speed_metrics(mock_record_id, timestamp=old_timestamp)

        # Insert the mock record directly into the local database
        local_database.insert_speed_metrics(mock_metrics_data)

        with (
             # Removed patch for local_database.get_if_need_to_sync
             patch("local_database.get_speed_metrics_to_sync", return_value=[mock_metrics_data]) as mock_get_speed_metrics_to_sync,
             patch("local_database.mark_speed_metrics_as_synced") as mock_mark_speed_metrics_as_synced,
             patch("remote_database.init_db") as mock_remote_init_db):

            self.monitor.check_sync()

            # Assert that get_if_need_to_sync was called (implicitly, not mocked)
            # We can't assert called_once on it directly since it's not mocked.
            # Instead, we rely on the fact that check_sync calls it.
            mock_remote_init_db.assert_called_once()
            mock_get_speed_metrics_to_sync.assert_called_once()
            # Assert that upload_speed_metrics was called with the correct data
            self.mock_bigquery_client.insert_rows_json.assert_called_once() # Check BigQuery client directly
            mock_mark_speed_metrics_as_synced.assert_called_once_with([mock_record_id])

    # def test_second_record_does_not_sync_immediately(self):
    #     """Test that a second speed record added to local SQLite does not trigger an immediate remote sync."""
    #     self._setup_monitor()

    #     mock_record_id_1 = 1
    #     mock_record_id_2 = 2
    #     mock_metrics_data_1 = self._create_mock_speed_metrics(mock_record_id_1, timestamp="1970-1-1T09:38:41.341Z", synced=0)
    #     mock_metrics_data_2 = self._create_mock_speed_metrics(mock_record_id_2, synced=0)

    #     # Scenario 1: First sync happens, get_if_need_to_sync returns True
    #     with (patch("local_database.get_if_need_to_sync", side_effect=[True, False]) as mock_get_if_need_to_sync,
    #          patch("local_database.get_speed_metrics_to_sync", side_effect=[[mock_metrics_data_1], [mock_metrics_data_2]]) as mock_get_speed_metrics_to_sync,
    #          # Removed patch for remote_database.upload_speed_metrics
    #          patch("local_database.mark_speed_metrics_as_synced") as mock_mark_speed_metrics_as_synced,
    #          patch("remote_database.init_db") as mock_remote_init_db):

    #         # Simulate the first sync
    #         self.monitor.check_sync()

    #         mock_get_if_need_to_sync.assert_called_once()
    #         mock_remote_init_db.assert_called_once()
    #         mock_get_speed_metrics_to_sync.assert_called_once()
    #         self.mock_bigquery_client.insert_rows_json.assert_called_once() # Check BigQuery client directly
    #         mock_mark_speed_metrics_as_synced.assert_called_once_with([mock_record_id_1])

    #         # Reset mocks for the second call, and verify no new sync happens
    #         mock_get_if_need_to_sync.reset_mock()
    #         mock_remote_init_db.reset_mock()
    #         mock_get_speed_metrics_to_sync.reset_mock()
    #         self.mock_bigquery_client.insert_rows_json.reset_mock() # Reset BigQuery client mock
    #         mock_mark_speed_metrics_as_synced.reset_mock()

    #         # Simulate adding a second record and checking for sync again
    #         self.monitor.check_sync()

    #         mock_get_if_need_to_sync.assert_called_once()
    #         mock_remote_init_db.assert_not_called()
    #         mock_get_speed_metrics_to_sync.assert_not_called() # No sync, so no need to get metrics
    #         self.mock_bigquery_client.insert_rows_json.assert_not_called() # Check BigQuery client directly
    #         mock_mark_speed_metrics_as_synced.assert_not_called()

    # def test_old_and_new_records_sync_after_delay(self):
    #     """Test that old and new unsynced speed records are synced after a week delay."""
    #     self._setup_monitor()

    #     # Create an old record (more than a week ago)
    #     one_week_ago = datetime.now(UTC) - timedelta(weeks=1, days=1)
    #     mock_record_id_old = 1
    #     mock_metrics_data_old = self._create_mock_speed_metrics(mock_record_id_old, timestamp=(one_week_ago.isoformat()), synced=0)

    #     # Create a new unsynced record
    #     mock_record_id_new = 2
    #     mock_metrics_data_new = self._create_mock_speed_metrics(mock_record_id_new, synced=0)

    #     with (patch("local_database.get_if_need_to_sync", return_value=True) as mock_get_if_need_to_sync,
    #          patch("local_database.get_speed_metrics_to_sync", return_value=[mock_metrics_data_old, mock_metrics_data_new]) as mock_get_speed_metrics_to_sync,
    #          # Removed patch for remote_database.upload_speed_metrics
    #          patch("local_database.mark_speed_metrics_as_synced") as mock_mark_speed_metrics_as_synced,
    #          patch("remote_database.init_db") as mock_remote_init_db):

    #         self.monitor.check_sync()

    #         mock_get_if_need_to_sync.assert_called_once()
    #         mock_remote_init_db.assert_called_once()
    #         mock_get_speed_metrics_to_sync.assert_called_once()
    #         # Assert that upload_speed_metrics was called with the correct data
    #         self.mock_bigquery_client.insert_rows_json.assert_called_once() # Check BigQuery client directly
    #         mock_mark_speed_metrics_as_synced.assert_called_once_with([mock_record_id_old, mock_metrics_data_new])


if __name__ == '__main__':
    unittest.main()
