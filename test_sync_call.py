import unittest
from unittest.mock import patch, MagicMock
import os
import sys

# Mock local_database before importing main
sys.modules['local_database'] = MagicMock()
import local_database as mock_local_database # Alias it to avoid confusion

from main import NetworkMonitor # Now main will import the mocked local_database

class TestNetworkMonitorSync(unittest.TestCase):

    @patch('google.cloud.bigquery.Client') # Patch the BigQuery client
    @patch('main.remote_database')
    @patch('main.NetworkMonitor._get_or_create_site_id', return_value='test_site_id')
    def setUp(self, mock_get_site_id, mock_remote_db, mock_bigquery_client):
        self.monitor = NetworkMonitor()
        self.mock_local_db = mock_local_database # Use the pre-mocked local_database
        self.mock_remote_db = mock_remote_db
        self.mock_bigquery_client = mock_bigquery_client # Store the mock for assertions if needed
        self.monitor.site_id = 'test_site_id'
        self.monitor.location = 'test_location'

        # Now set the return values for the methods on the mock_local_db
        self.mock_local_db.get_if_need_to_sync.return_value = False # Default, can be overridden in tests
        mock_local_db_get_if_need_to_sync = False
        self.mock_local_db.get_ping_metrics_to_sync.return_value = [] # Default
        self.mock_local_db.get_speed_metrics_to_sync.return_value = [] # Default
        self.mock_local_db.mark_ping_metrics_as_synced.return_value = None # Default
        self.mock_local_db.mark_speed_metrics_as_synced.return_value = None # Default
        self.mock_local_db.get_never_synced.return_value = False # Default, to prevent internal calls

    def test_check_sync_no_metrics_to_sync(self):
        """Test check_sync when no metrics need to be synced."""
        self.mock_local_db.get_if_need_to_sync.return_value = False
        self.mock_local_db.get_ping_metrics_to_sync.return_value = []
        self.mock_local_db.get_speed_metrics_to_sync.return_value = []

        self.monitor.check_sync()

        self.mock_local_db.get_if_need_to_sync.assert_called_once()
        self.mock_local_db.get_ping_metrics_to_sync.assert_not_called()
        self.mock_local_db.get_speed_metrics_to_sync.assert_not_called()
        self.mock_remote_db.init_db.assert_called_once() # init_db should be called to check for tables
        self.mock_remote_db.upload_ping_metrics.assert_not_called() # Corrected from insert_ping_metrics
        self.mock_remote_db.upload_speed_metrics.assert_not_called() # Corrected from insert_speed_metrics
        self.mock_local_db.mark_ping_metrics_as_synced.assert_not_called()
        self.mock_local_db.mark_speed_metrics_as_synced.assert_not_called()

    def test_check_sync_both_metrics(self):
        """Test check_sync when both ping and speed metrics are available for syncing."""
        self.mock_local_db.get_if_need_to_sync.return_value = True
        ping_metrics = [{'id': 1, 'timestamp': '2026-01-01T00:00:00', 'site_id': 'test_site_id', 'location': 'test_location', 'google_up': 1}]
        speed_metrics = [{'id': 2, 'timestamp': '2026-01-01T00:00:00', 'site_id': 'test_site_id', 'location': 'test_location', 'download_mbps': 100}]
        self.mock_local_db.get_ping_metrics_to_sync.return_value = ping_metrics
        self.mock_local_db.get_speed_metrics_to_sync.return_value = speed_metrics

        self.monitor.check_sync()

        self.mock_local_db.get_if_need_to_sync.assert_called_once()
        self.mock_local_db.get_ping_metrics_to_sync.assert_called_once()
        self.mock_local_db.get_speed_metrics_to_sync.assert_called_once()
        self.mock_remote_db.init_db.assert_called_once()
        self.mock_remote_db.upload_ping_metrics.assert_called_once() # Corrected from insert_ping_metrics
        self.mock_remote_db.upload_speed_metrics.assert_called_once() # Corrected from insert_speed_metrics
        self.mock_local_db.mark_ping_metrics_as_synced.assert_not_called()
        self.mock_local_db.mark_speed_metrics_as_synced.assert_called_once_with([2])

if __name__ == '__main__':
    unittest.main()
