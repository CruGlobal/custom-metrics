import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import json
import requests
from datetime import datetime, timedelta, UTC
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

        self.local_db_init_patcher = patch("local_database.init_db")
        self.mock_local_db_init = self.local_db_init_patcher.start()
        
        self.remote_db_init_patcher = patch("remote_database.init_db")
        self.mock_remote_db_init = self.remote_db_init_patcher.start()

        # self.monitor will be created in individual tests where needed
        self.monitor = None

    def tearDown(self):
        self.site_id_file_patcher.stop()
        self.path_exists_patcher.stop()
        self.open_patcher.stop()
        self.requests_get_patcher.stop()
        self.getenv_patcher.stop()
        self.makedirs_patcher.stop()
        self.local_db_init_patcher.stop()
        self.remote_db_init_patcher.stop()

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
            "ip": "123.45.67.89",
            "city": "Test City",
            "region": "Test Region",
            "country": "TC"
        }
        mock_response.raise_for_status.return_value = None
        # Clear side_effect and set the first call
        self.mock_requests_get.side_effect = [mock_response]

        self.monitor = NetworkMonitor() # Initialize monitor for this test

    def _create_mock_ping_metrics(self, metric_id, timestamp=None, synced=0):
        if timestamp is None:
            timestamp = datetime.now(UTC).isoformat()
        return {
            "id": metric_id,
            "timestamp": timestamp,
            "site_id": "test_site_id",
            "location": "Test City, Test Region, TC",
            "ip_address": "123.45.67.89",
            "google_up": "1", "apple_up": "1", "github_up": "1",
            "pihole_up": "1", "node_up": "1", "speedtest_up": "1",
            "http_latency": "0.1", "http_samples": "10", "http_time": "0.5",
            "http_content_length": "1000", "http_duration": "0.2",
            "synced": synced
        }

    def test_smoke(self):
        """Basic smoke test to verify test infrastructure."""
        self._setup_monitor()
        self.assertTrue(True)
        self.assertEqual(1 + 1, 2)
        self.assertIsNotNone(self.monitor)
        self.assertIsInstance(self.monitor, NetworkMonitor)
        self.assertEqual(self.monitor.location, "Test City, Test Region, TC")
        self.assertEqual(self.monitor.ip_address, "123.45.67.89")

    def test_first_record_syncs_to_remote(self):
        """Test that a single record added to local SQLite is synced to remote and marked as synced."""
        self._setup_monitor()

        mock_record_id = 1
        mock_metrics_data = self._create_mock_ping_metrics(mock_record_id, synced=0)

        with (patch("local_database.get_if_need_to_sync", return_value=True) as mock_get_if_need_to_sync,
             patch("local_database.get_ping_metrics_to_sync", return_value=[mock_metrics_data]) as mock_get_ping_metrics_to_sync,
             patch("remote_database.insert_ping_metrics") as mock_remote_insert_ping_metrics,
             patch("local_database.mark_ping_metrics_as_synced") as mock_mark_ping_metrics_as_synced,
             patch("remote_database.init_db") as mock_remote_init_db):

            self.monitor.check_sync()

            mock_get_if_need_to_sync.assert_called_once()
            mock_remote_init_db.assert_called_once()
            mock_get_ping_metrics_to_sync.assert_called_once()
            mock_remote_insert_ping_metrics.assert_called_once_with([mock_metrics_data])
            mock_mark_ping_metrics_as_synced.assert_called_once_with([mock_record_id])

    def test_second_record_does_not_sync_immediately(self):
        """Test that a second record added to local SQLite does not trigger an immediate remote sync."""
        self._setup_monitor()

        mock_record_id_1 = 1
        mock_record_id_2 = 2
        mock_metrics_data_1 = self._create_mock_ping_metrics(mock_record_id_1, synced=0)
        mock_metrics_data_2 = self._create_mock_ping_metrics(mock_record_id_2, synced=0)

        # Scenario 1: First sync happens, get_if_need_to_sync returns True
        with (patch("local_database.get_if_need_to_sync", side_effect=[True, False]) as mock_get_if_need_to_sync,
             patch("local_database.get_ping_metrics_to_sync", side_effect=[[mock_metrics_data_1], [mock_metrics_data_2]]) as mock_get_ping_metrics_to_sync,
             patch("remote_database.insert_ping_metrics") as mock_remote_insert_ping_metrics,
             patch("local_database.mark_ping_metrics_as_synced") as mock_mark_ping_metrics_as_synced,
             patch("remote_database.init_db") as mock_remote_init_db):

            # Simulate the first sync
            self.monitor.check_sync()

            mock_get_if_need_to_sync.assert_called_once()
            mock_remote_init_db.assert_called_once()
            mock_get_ping_metrics_to_sync.assert_called_once()
            mock_remote_insert_ping_metrics.assert_called_once_with([mock_metrics_data_1])
            mock_mark_ping_metrics_as_synced.assert_called_once_with([mock_record_id_1])

            # Reset mocks for the second call, and verify no new sync happens
            mock_get_if_need_to_sync.reset_mock()
            mock_remote_init_db.reset_mock()
            mock_get_ping_metrics_to_sync.reset_mock()
            mock_remote_insert_ping_metrics.reset_mock()
            mock_mark_ping_metrics_as_synced.reset_mock()

            # Simulate adding a second record and checking for sync again
            self.monitor.check_sync()

            mock_get_if_need_to_sync.assert_called_once()
            mock_remote_init_db.assert_not_called()
            mock_get_ping_metrics_to_sync.assert_not_called() # No sync, so no need to get metrics
            mock_remote_insert_ping_metrics.assert_not_called()
            mock_mark_ping_metrics_as_synced.assert_not_called()

    def test_old_and_new_records_sync_after_delay(self):
        """Test that old and new unsynced records are synced after a week delay."""
        self._setup_monitor()

        # Create an old record (more than a week ago)
        one_week_ago = datetime.now(UTC) - timedelta(weeks=1, days=1)
        mock_record_id_old = 1
        mock_metrics_data_old = self._create_mock_ping_metrics(mock_record_id_old, timestamp=one_week_ago.isoformat(), synced=0)

        # Create a new unsynced record
        mock_record_id_new = 2
        mock_metrics_data_new = self._create_mock_ping_metrics(mock_record_id_new, synced=0)

        with (patch("local_database.get_if_need_to_sync", return_value=True) as mock_get_if_need_to_sync,
             patch("local_database.get_ping_metrics_to_sync", return_value=[mock_metrics_data_old, mock_metrics_data_new]) as mock_get_ping_metrics_to_sync,
             patch("remote_database.insert_ping_metrics") as mock_remote_insert_ping_metrics,
             patch("local_database.mark_ping_metrics_as_synced") as mock_mark_ping_metrics_as_synced,
             patch("remote_database.init_db") as mock_remote_init_db):

            self.monitor.check_sync()

            mock_get_if_need_to_sync.assert_called_once()
            mock_remote_init_db.assert_called_once()
            mock_get_ping_metrics_to_sync.assert_called_once()
            
            # Both old and new unsynced records should be sent
            mock_remote_insert_ping_metrics.assert_called_once_with([mock_metrics_data_old, mock_metrics_data_new])
            mock_mark_ping_metrics_as_synced.assert_called_once_with([mock_record_id_old, mock_record_id_new])