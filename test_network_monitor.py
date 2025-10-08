import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import json
import requests
from datetime import datetime
import main
from main import (
    NetworkMonitor, SITE_ID_FILE, PING_METRICS, SPEED_METRICS,
    PING_TABLE, SPEED_TABLE
)
import database
import asyncio

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

        self.turso_client_patcher = patch('main.create_client')
        self.mock_create_client = self.turso_client_patcher.start()
        self.mock_turso_client = MagicMock()
        self.mock_create_client.return_value = self.mock_turso_client

        self.requests_get_patcher = patch('main.requests.get')
        self.requests_get_patcher.start()

        self.getenv_patcher = patch('main.os.getenv')
        self.mock_getenv = self.getenv_patcher.start()
        self.mock_getenv.side_effect = lambda key, default=None: {
            'TURSO_DATABASE_URL': 'libsql://test.turso.io',
            'TURSO_AUTH_TOKEN': 'test-token',
            'PING_TABLE': 'test-ping',
            'SPEED_TABLE': 'test-speed',
            'PROMETHEUS_URL': 'http://test-prometheus:9090',
            'LOCATION': 'test-location'
        }.get(key, default)

        self.makedirs_patcher = patch('main.os.makedirs')
        self.makedirs_patcher.start()

        # self.monitor will be created in individual tests where needed
        self.monitor = None

    def tearDown(self):
        self.site_id_file_patcher.stop()
        self.path_exists_patcher.stop()
        self.open_patcher.stop()
        self.turso_client_patcher.stop()
        self.requests_get_patcher.stop()
        self.getenv_patcher.stop()
        self.makedirs_patcher.stop()

    def test_smoke(self):
        """Basic smoke test to verify test infrastructure."""
        self.monitor = NetworkMonitor() # Initialize monitor for this test
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
        self.mock_create_client.reset_mock() # Clear mock calls from NetworkMonitor() in this test

    def test_get_or_create_site_id_new(self):
        """Test creating new site ID."""
        os.path.exists.return_value = False
        
        with patch('builtins.open', mock_open()) as mock_file:
            monitor = NetworkMonitor()
            mock_file.assert_called_with('/tmp/test_site_id', 'w')
            self.assertIsNotNone(monitor.site_id)
        self.mock_create_client.reset_mock() # Clear mock calls from NetworkMonitor() in this test

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

        self.monitor = NetworkMonitor() # Initialize monitor for this test
        result = self.monitor._query_prometheus("test_query")
        self.assertEqual(result, mock_response.json.return_value)

    def test_query_prometheus_failure(self):
        """Test failed Prometheus query."""
        requests.get.side_effect = Exception("Connection error")
        self.monitor = NetworkMonitor() # Initialize monitor for this test
        result = self.monitor._query_prometheus("test_query")
        self.assertIsNone(result)

    @patch('database.insert_ping_metrics')
    def test_insert_ping_metrics_success(self, mock_insert_ping_metrics):
        """Test successful ping metrics insertion."""
        test_metrics = {
            'google_up': 1.0,
            'apple_up': 1.0,
            'github_up': 1.0,
            'http_latency': 0.1
        }

        self.monitor = NetworkMonitor() # Initialize monitor for this test
        self.monitor._insert_ping_metrics(test_metrics)
        mock_insert_ping_metrics.assert_called_once()

    @patch('database.insert_speed_metrics')
    def test_insert_speed_metrics_success(self, mock_insert_speed_metrics):
        """Test successful speed metrics insertion."""
        test_metrics = {
            'download_mbps': 291.0,
            'upload_mbps': 336.0,
            'ping_ms': 1.3,
            'jitter_ms': 0.3
        }

        self.monitor = NetworkMonitor() # Initialize monitor for this test
        self.monitor._insert_speed_metrics(test_metrics)
        mock_insert_speed_metrics.assert_called_once()

    @patch('database.insert_ping_metrics')
    def test_collect_ping_metrics(self, mock_insert_ping_metrics):
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

        self.monitor = NetworkMonitor() # Initialize monitor for this test
        self.monitor.collect_ping_metrics()
        
        # Verify that insert_ping_metrics was called
        mock_insert_ping_metrics.assert_called_once()

    @patch('database.insert_speed_metrics')
    def test_collect_speed_metrics_with_data(self, mock_insert_speed_metrics):
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

        self.monitor = NetworkMonitor() # Initialize monitor for this test
        self.monitor.collect_speed_metrics()
        
        # Verify that insert_speed_metrics was called
        mock_insert_speed_metrics.assert_called_once()

    @patch('database.get_all_ping_metrics')
    @patch('database.get_all_speed_metrics')
    @patch('database.clear_ping_metrics')
    @patch('database.clear_speed_metrics')
    async def test_mass_upload_metrics(self, mock_clear_speed_metrics, mock_clear_ping_metrics, mock_get_all_speed_metrics, mock_get_all_ping_metrics):
        """Test uploading metrics."""
        # Mock ping metrics data
        mock_get_all_ping_metrics.return_value = [
            {
                'timestamp': '2023-01-01T00:00:00',
                'site_id': 'test-site-id',
                'location': 'test-location',
                'google_up': 1.0,
                'apple_up': 1.0,
                'github_up': 1.0,
                'pihole_up': 1.0,
                'node_up': 1.0,
                'speedtest_up': 1.0,
                'http_latency': 0.1,
                'http_samples': 100,
                'http_time': 0.5,
                'http_content_length': 1000,
                'http_duration': 0.2
            }
        ]
        
        # Mock speed metrics data
        mock_get_all_speed_metrics.return_value = [
            {
                'timestamp': '2023-01-01T00:00:00',
                'site_id': 'test-site-id',
                'location': 'test-location',
                'download_mbps': 291.0,
                'upload_mbps': 336.0,
                'ping_ms': 1.3,
                'jitter_ms': 0.3
            }
        ]
        
        # Call the method to test
        self.monitor = NetworkMonitor() # Initialize monitor for this test
        asyncio.run(self.monitor._mass_upload_metrics())
        
        # Verify that the methods were called
        mock_get_all_ping_metrics.assert_called_once()
        mock_get_all_speed_metrics.assert_called_once()
        
        # Verify Turso client execute was called twice
        self.assertEqual(self.mock_create_client.call_count, 1)
        self.assertEqual(self.mock_turso_client.execute.call_count, 2)
        
        # Assert the SQL queries and parameters for ping metrics
        ping_call_args = self.mock_turso_client.execute.call_args_list[0]
        self.assertIn("INSERT INTO test-ping", ping_call_args.args[0])
        self.assertIn("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ping_call_args.args[0])
        self.assertEqual(ping_call_args.args[1][0], 'test-site-id')
        self.assertEqual(ping_call_args.args[1][1], '2023-01-01T00:00:00')
        
        # Assert the SQL queries and parameters for speed metrics
        speed_call_args = self.mock_turso_client.execute.call_args_list[1]
        self.assertIn("INSERT INTO test-speed", speed_call_args.args[0])
        self.assertIn("VALUES (?, ?, ?, ?, ?, ?, ?)", speed_call_args.args[0])
        self.assertEqual(speed_call_args.args[1][0], 'test-site-id')
        self.assertEqual(speed_call_args.args[1][1], '2023-01-01T00:00:00')

        mock_clear_ping_metrics.assert_called_once()
        mock_clear_speed_metrics.assert_called_once()
        
    @patch('database.get_all_ping_metrics')
    @patch('database.get_all_speed_metrics')
    @patch('database.clear_ping_metrics')
    @patch('database.clear_speed_metrics')
    async def test_upload_metrics_calls_mass_upload(self, mock_clear_speed_metrics, mock_clear_ping_metrics, mock_get_all_speed_metrics, mock_get_all_ping_metrics):
        """Test that upload_metrics calls _mass_upload_metrics."""
        # Mock ping metrics data
        mock_get_all_ping_metrics.return_value = []
        
        # Mock speed metrics data
        mock_get_all_speed_metrics.return_value = []
        
        # Call the method to test
        self.monitor = NetworkMonitor() # Initialize monitor for this test
        with patch.object(self.monitor, '_mass_upload_metrics') as mock_mass_upload_metrics:
            asyncio.run(self.monitor.upload_metrics())
            mock_mass_upload_metrics.assert_called_once()

    def test_init_turso_client_success(self):
        """Test successful Turso client initialization."""
        self.mock_getenv.side_effect = lambda key, default=None: {
            'TURSO_DATABASE_URL': 'libsql://test.turso.io',
            'TURSO_AUTH_TOKEN': 'test-token',
            'LOCATION': 'test-location'
        }.get(key, default)
        
        self.mock_create_client.reset_mock() # Clear any previous calls
        monitor = NetworkMonitor()
        self.mock_create_client.assert_called_once_with(
            url='libsql://test.turso.io',
            auth_token='test-token'
        )
        self.assertEqual(monitor.turso_client, self.mock_turso_client)

    def test_init_turso_client_missing_env_vars(self):
        """Test Turso client initialization with missing environment variables."""
        self.mock_getenv.side_effect = lambda key, default=None: {
            'TURSO_DATABASE_URL': None,
            'TURSO_AUTH_TOKEN': None,
            'LOCATION': 'test-location'
        }.get(key, default)
        
        self.mock_create_client.reset_mock() # Clear any previous calls
        with self.assertRaises(ValueError) as cm:
            NetworkMonitor()
        self.assertIn("Turso database URL and auth token must be set as environment variables.", str(cm.exception))
        self.mock_create_client.assert_not_called()

if __name__ == '__main__':
    unittest.main()
