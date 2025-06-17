import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import json
import requests
from datetime import datetime
import main
from main import (
    NetworkMonitor, SITE_ID_FILE, PING_METRICS, SPEED_METRICS,
    BIGQUERY_PROJECT, BIGQUERY_DATASET, PING_TABLE, SPEED_TABLE
)

class TestNetworkMonitor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Patch SITE_ID_FILE to a test path
        self.site_id_file_patcher = patch('main.SITE_ID_FILE', '/tmp/test_site_id')
        self.site_id_file_patcher.start()

        self.path_exists_patcher = patch('main.os.path.exists', return_value=False)
        self.mock_path_exists = self.path_exists_patcher.start()

        self.open_patcher = patch('builtins.open', mock_open())
        self.mock_open = self.open_patcher.start()

        self.bigquery_client_patcher = patch('main.bigquery.Client')
        self.bigquery_client_patcher.start()

        self.requests_get_patcher = patch('main.requests.get')
        self.requests_get_patcher.start()

        self.getenv_patcher = patch('main.os.getenv')
        self.mock_getenv = self.getenv_patcher.start()
        self.mock_getenv.side_effect = lambda key, default=None: {
            'BIGQUERY_PROJECT': 'test-project',
            'BIGQUERY_DATASET': 'test-dataset',
            'PING_TABLE': 'test-ping',
            'SPEED_TABLE': 'test-speed',
            'PROMETHEUS_URL': 'http://test-prometheus:9090',
            'LOCATION': 'test-location'
        }.get(key, default)

        self.makedirs_patcher = patch('main.os.makedirs')
        self.makedirs_patcher.start()

        # Create instance of NetworkMonitor
        self.monitor = NetworkMonitor()

    def tearDown(self):
        self.site_id_file_patcher.stop()
        self.path_exists_patcher.stop()
        self.open_patcher.stop()
        self.bigquery_client_patcher.stop()
        self.requests_get_patcher.stop()
        self.getenv_patcher.stop()
        self.makedirs_patcher.stop()

    def test_smoke(self):
        """Basic smoke test to verify test infrastructure."""
        self.assertTrue(True)
        self.assertEqual(1 + 1, 2)
        self.assertIsNotNone(self.monitor)
        self.assertIsInstance(self.monitor, NetworkMonitor)
        self.assertEqual(self.monitor.location, 'test-location')

    def test_get_or_create_site_id_existing(self):
        """Test getting existing site ID."""
        test_site_id = "test-uuid-123"
        os.path.exists.return_value = True
        
        with patch('builtins.open', mock_open(read_data=test_site_id)):
            monitor = NetworkMonitor()
            self.assertEqual(monitor.site_id, test_site_id)

    def test_get_or_create_site_id_new(self):
        """Test creating new site ID."""
        os.path.exists.return_value = False
        
        with patch('builtins.open', mock_open()) as mock_file:
            monitor = NetworkMonitor()
            mock_file.assert_called_with('/tmp/test_site_id', 'w')
            self.assertIsNotNone(monitor.site_id)

    def test_query_prometheus_success(self):
        """Test successful Prometheus query."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'data': {
                'result': [{
                    'metric': {'instance': 'test-instance'},
                    'value': ['1234567890', '1.0']
                }]
            }
        }
        mock_response.raise_for_status.return_value = None
        requests.get.return_value = mock_response

        result = self.monitor._query_prometheus("test_query")
        self.assertEqual(result, mock_response.json.return_value)

    def test_query_prometheus_failure(self):
        """Test failed Prometheus query."""
        requests.get.side_effect = Exception("Connection error")
        result = self.monitor._query_prometheus("test_query")
        self.assertIsNone(result)

    def test_insert_ping_metrics_success(self):
        """Test successful ping metrics insertion."""
        mock_client = MagicMock()
        self.monitor.bigquery_client = mock_client
        mock_client.insert_rows_json.return_value = []

        test_metrics = {
            'google_up': 1.0,
            'apple_up': 1.0,
            'github_up': 1.0,
            'http_latency': 0.1
        }

        self.monitor._insert_ping_metrics(test_metrics)
        mock_client.insert_rows_json.assert_called_once()
        args = mock_client.insert_rows_json.call_args[0]
        self.assertEqual(args[0], f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{PING_TABLE}")

    def test_insert_speed_metrics_success(self):
        """Test successful speed metrics insertion."""
        mock_client = MagicMock()
        self.monitor.bigquery_client = mock_client
        mock_client.insert_rows_json.return_value = []

        test_metrics = {
            'download_mbps': 291.0,
            'upload_mbps': 336.0,
            'ping_ms': 1.3,
            'jitter_ms': 0.3
        }

        self.monitor._insert_speed_metrics(test_metrics)
        mock_client.insert_rows_json.assert_called_once()
        args = mock_client.insert_rows_json.call_args[0]
        self.assertEqual(args[0], f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{SPEED_TABLE}")

    def test_collect_ping_metrics(self):
        """Test collecting ping metrics."""
        # Mock successful Prometheus queries
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'data': {
                'result': [{
                    'metric': {'instance': 'test-instance'},
                    'value': ['1234567890', '1.0']
                }]
            }
        }
        mock_response.raise_for_status.return_value = None
        requests.get.return_value = mock_response

        # Mock successful BigQuery insertion
        mock_client = MagicMock()
        self.monitor.bigquery_client = mock_client
        mock_client.insert_rows_json.return_value = []

        self.monitor.collect_ping_metrics()
        
        # Verify that insert_rows_json was called
        mock_client.insert_rows_json.assert_called_once()
        args = mock_client.insert_rows_json.call_args[0]
        self.assertEqual(args[0], f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{PING_TABLE}")

    def test_collect_speed_metrics_with_data(self):
        """Test collecting speed metrics when data is available."""
        # Mock successful speedtest data
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'data': {
                'result': [{
                    'metric': {'instance': 'speedtest:9798'},
                    'value': ['1234567890', '291000000']  # 291 Mbps in bits
                }]
            }
        }
        mock_response.raise_for_status.return_value = None
        requests.get.return_value = mock_response

        # Mock successful BigQuery insertion
        mock_client = MagicMock()
        self.monitor.bigquery_client = mock_client
        mock_client.insert_rows_json.return_value = []

        self.monitor.collect_speed_metrics()
        
        # Verify that insert_rows_json was called
        mock_client.insert_rows_json.assert_called_once()
        args = mock_client.insert_rows_json.call_args[0]
        self.assertEqual(args[0], f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{SPEED_TABLE}")

    def test_collect_speed_metrics_no_data(self):
        """Test collecting speed metrics when no data is available."""
        # Mock no speedtest data
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'data': {
                'result': []
            }
        }
        mock_response.raise_for_status.return_value = None
        requests.get.return_value = mock_response

        # Mock BigQuery client
        mock_client = MagicMock()
        self.monitor.bigquery_client = mock_client

        self.monitor.collect_speed_metrics()
        
        # Verify that insert_rows_json was not called
        mock_client.insert_rows_json.assert_not_called()

if __name__ == '__main__':
    unittest.main() 