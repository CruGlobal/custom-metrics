import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import json
import requests
from datetime import datetime
import main
from main import NetworkMonitor, SITE_ID_FILE, METRICS, BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE

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
            'BIGQUERY_TABLE': 'test-table',
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

    def test_parse_prometheus_result(self):
        """Test parsing Prometheus result."""
        test_result = {
            'data': {
                'result': [{
                    'metric': {
                        'instance': 'test-instance',
                        'job': 'test-job',
                        'ping_target': 'google',
                        'ping_time': '0.1',
                        'ping_samples': '10'
                    },
                    'value': ['1234567890', '1.0']
                }]
            }
        }

        parsed_data = self.monitor._parse_prometheus_result(test_result, 'test_metric')
        self.assertEqual(parsed_data['metric'], 'test_metric')
        self.assertEqual(parsed_data['instance'], 'test-instance')
        self.assertEqual(parsed_data['job'], 'test-job')
        self.assertEqual(parsed_data['google_ping'], True)
        self.assertEqual(parsed_data['google_time'], 0.1)
        self.assertEqual(parsed_data['google_samples'], 10.0)

    def test_insert_metric_success(self):
        """Test successful metric insertion."""
        mock_client = MagicMock()
        self.monitor.bigquery_client = mock_client
        mock_client.insert_rows_json.return_value = []

        test_metric_data = {
            'metric': 'test_metric',
            'value': '1.0',
            'instance': 'test-instance',
            'job': 'test-job'
        }

        self.monitor._insert_metric(test_metric_data)
        mock_client.insert_rows_json.assert_called_once()

    def test_insert_metric_failure(self):
        """Test failed metric insertion."""
        mock_client = MagicMock()
        self.monitor.bigquery_client = mock_client
        mock_client.insert_rows_json.side_effect = Exception("Insertion error")

        test_metric_data = {
            'metric': 'test_metric',
            'value': '1.0',
            'instance': 'test-instance',
            'job': 'test-job'
        }

        self.monitor._insert_metric(test_metric_data)
        # Should not raise exception, just log error

    def test_collect_metrics(self):
        """Test collecting all metrics."""
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

        self.monitor.collect_metrics()
        
        # Verify that insert_rows_json was called for each metric
        expected_calls = len(METRICS) + 1  # +1 for the 'up' metric
        self.assertEqual(mock_client.insert_rows_json.call_count, expected_calls)

if __name__ == '__main__':
    unittest.main() 