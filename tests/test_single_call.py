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
        self.mock_local_db = mock_local_database # Initialize mock_local_database here
        self.mock_local_db.get_if_need_to_sync.return_value = False 

    def test_check_sync_no_metrics_to_sync(self):
        """Test check_sync when no metrics need to be synced."""
        self.monitor = NetworkMonitor()
        self.mock_local_db.get_if_need_to_sync.return_value = False
        self.mock_local_db.get_ping_metrics_to_sync.return_value = []
        self.mock_local_db.get_speed_metrics_to_sync.return_value = []

        self.monitor.check_sync()

        self.mock_local_db.get_if_need_to_sync.assert_called_once()
