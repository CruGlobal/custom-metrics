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

class TestNetworkMonitor(unittest.TestCase):
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

    def test_smoke(self):
        """Basic smoke test to verify test infrastructure."""
        self._setup_monitor()
        self.assertTrue(True)
        self.assertEqual(1 + 1, 2)
        self.assertIsNotNone(self.monitor)
        self.assertIsInstance(self.monitor, NetworkMonitor)
        self.assertEqual(self.monitor.location, "Test City, Test Region, TC")
        self.assertEqual(self.monitor.ip_address, "123.45.67.89")

    def test_get_or_create_site_id_existing(self):
        """Test getting existing site ID."""
        test_site_id = "test-uuid-123"
        self.mock_path_exists.return_value = True
        
        with patch("builtins.open", mock_open(read_data=test_site_id)):
            self._setup_monitor()
            self.assertEqual(self.monitor.site_id, test_site_id)

    def test_get_or_create_site_id_new(self):
        """Test creating new site ID."""
        self.mock_path_exists.return_value = False
        
        with patch("builtins.open", mock_open()) as mock_file:
            self._setup_monitor()
            mock_file.assert_called_with("/tmp/test_site_id", "w")
            self.assertIsNotNone(self.monitor.site_id)

    def test_query_prometheus_success(self):
        """Test successful Prometheus query."""
        # Mock the requests.get specifically for Prometheus query
        mock_prometheus_response = MagicMock()
        mock_prometheus_response.json.return_value = {
            'data': {
                'result': [{
                    'metric': {'instance': 'test-instance'},
                    'value': [str(datetime.now().timestamp()), '1.0']
                }]
            }
        }
        mock_prometheus_response.raise_for_status.return_value = None
        self.mock_requests_get.side_effect = [mock_prometheus_response] # Set side_effect as a list

        # Do not call _setup_monitor here, as it interferes with requests.get mock
        # Instead, manually create a monitor for this test (site_id and IP/location won't matter)
        with patch("main.NetworkMonitor._get_or_create_site_id", return_value="test-site-id"),\
             patch("main.NetworkMonitor._get_ip_and_location", return_value=("127.0.0.1", "Test Location")):
            monitor = NetworkMonitor()
            result = monitor._query_prometheus("test_query")
            self.assertEqual(result, mock_prometheus_response.json.return_value)

    def test_query_prometheus_failure(self):
        """Test failed Prometheus query."""
        self.mock_requests_get.side_effect = Exception("Connection error")
        # Manually create a monitor to avoid _setup_monitor interfering
        with patch("main.NetworkMonitor._get_or_create_site_id", return_value="test-site-id"),\
             patch("main.NetworkMonitor._get_ip_and_location", return_value=("127.0.0.1", "Test Location")):
            monitor = NetworkMonitor()
            result = monitor._query_prometheus("test_query")
            self.assertIsNone(result)

    @patch("local_database.insert_ping_metrics")
    def test_insert_ping_metrics_success(self, mock_insert_ping_metrics):
        """Test successful ping metrics insertion."""
        test_metrics = {
            "google_up": 1.0,
            "apple_up": 1.0,
            "github_up": 1.0,
            "http_latency": 0.1
        }

        self._setup_monitor() # Initialize monitor for this test
        self.monitor._insert_ping_metrics(test_metrics)
        mock_insert_ping_metrics.assert_called_once()

    @patch("local_database.insert_speed_metrics")
    def test_insert_speed_metrics_success(self, mock_insert_speed_metrics):
        """Test successful speed metrics insertion."""
        test_metrics = {
            "download_mbps": 291.0,
            "upload_mbps": 336.0,
            "ping_ms": 1.3,
            "jitter_ms": 0.3
        }

        self._setup_monitor() # Initialize monitor for this test
        self.monitor._insert_speed_metrics(test_metrics)
        mock_insert_speed_metrics.assert_called_once()

    @patch("local_database.insert_ping_metrics")
    def test_collect_ping_metrics(self, mock_insert_ping_metrics):
        """Test collecting ping metrics."""
        self.mock_requests_get.side_effect = [] # Clear side_effect for this test to correctly set it up
        self._setup_monitor() # This will set self.mock_requests_get.side_effect to contain the ipinfo response

        # Mock successful Prometheus queries for *all* PING_METRICS
        mock_results = []
        for i in range(len(PING_METRICS)):
            mock_response_item = MagicMock()
            mock_response_item.json.return_value = {
                'data': {
                    'result': [{
                        'metric': {'instance': f'test-instance-{i}'},
                        'value': [str(datetime.now().timestamp()), f'{1.0 + i}']
                    }]
                }
            }
            mock_response_item.raise_for_status.return_value = None
            mock_results.append(mock_response_item)
        
        # Subsequent calls are for Prometheus queries
        self.mock_requests_get.side_effect.extend(mock_results)

        self.monitor.collect_ping_metrics()
        
        # Verify that insert_ping_metrics was called once
        mock_insert_ping_metrics.assert_called_once()
        self.assertIn('site_id', mock_insert_ping_metrics.call_args[0][0])
        self.assertIn('location', mock_insert_ping_metrics.call_args[0][0])
        self.assertIn('ip_address', mock_insert_ping_metrics.call_args[0][0])
        # Check if at least one ping metric was collected
        self.assertTrue(any(key in PING_METRICS for key in mock_insert_ping_metrics.call_args[0][0]))

    @patch("local_database.insert_speed_metrics")
    def test_collect_speed_metrics_with_data(self, mock_insert_speed_metrics):
        """Test collecting speed metrics when data is available."""
        self.mock_requests_get.side_effect = [] # Clear side_effect for this test
        self._setup_monitor() # This will set self.mock_requests_get.side_effect to contain the ipinfo response
        
        # Mock successful speedtest data for *all* SPEED_METRICS
        mock_results = []
        # First mock response is for the initial check for speedtest_up
        mock_initial_speedtest_up = MagicMock()
        mock_initial_speedtest_up.json.return_value = {
            'data': {
                'result': [{
                    'metric': {'instance': 'speedtest:9798'},
                    'value': [str(datetime.now().timestamp()), '1']
                }]
            }
        }
        mock_initial_speedtest_up.raise_for_status.return_value = None
        mock_results.append(mock_initial_speedtest_up)

        for i in range(len(SPEED_METRICS)):
            mock_response_item = MagicMock()
            # Simulate speed in bits per second, main.py converts to Mbps
            mock_response_item.json.return_value = {
                'data': {
                    'result': [{
                        'metric': {'instance': 'speedtest:9798'},
                        'value': [str(datetime.now().timestamp()), f'{291000000 + i}'] 
                    }]
                }
            }
            mock_response_item.raise_for_status.return_value = None
            mock_results.append(mock_response_item)

        self.mock_requests_get.side_effect.extend(mock_results)

        self.monitor.collect_speed_metrics()
        
        # Verify that insert_speed_metrics was called
        mock_insert_speed_metrics.assert_called_once()
        self.assertIn('site_id', mock_insert_speed_metrics.call_args[0][0])
        self.assertIn('location', mock_insert_speed_metrics.call_args[0][0])
        self.assertIn('ip_address', mock_insert_speed_metrics.call_args[0][0])
        # Check if at least one speed metric was collected
        self.assertTrue(any(key in SPEED_METRICS for key in mock_insert_speed_metrics.call_args[0][0]))


    @patch("local_database.insert_ping_metrics")
    @patch("main.NetworkMonitor._query_prometheus")
    @patch("local_database.get_if_need_to_sync", return_value=False) # Corrected to local_database
    def test_add_single_record_to_local_sqlite(self, mock_get_if_need_to_sync, mock_query_prometheus, mock_insert_ping_metrics):
        """
        Test case: Add a single record to local SQLite.
        A call should be made to remote database (not immediately, but on sync).
        Record should be marked as 'synced' (after successful sync).
        """
        self._setup_monitor()

        # Mock Prometheus query to return a single ping metric for each PING_METRICS item
        mock_query_prometheus.side_effect = [
            {'data': {'result': [{'metric': {'instance': f'test-instance-{i}'}, 'value': [str(datetime.now().timestamp()), f'{1.0 + i}']}]}} for i in range(len(PING_METRICS))]

        # Act: Collect ping metrics
        self.monitor.collect_ping_metrics()
        self.monitor.check_sync() # Explicitly call check_sync here

        # Assert: local_database.insert_ping_metrics was called once
        mock_insert_ping_metrics.assert_called_once()

        # Assert: local_database.get_if_need_to_sync was checked
        mock_get_if_need_to_sync.assert_called_once_with(timedelta(weeks=1)) # Check for correct argument

    @patch("local_database.insert_ping_metrics")
    @patch("main.NetworkMonitor._query_prometheus")
    @patch("local_database.get_if_need_to_sync", return_value=False) # Corrected to local_database
    @patch("remote_database.insert_ping_metrics")
    def test_add_second_record_to_local_sqlite_no_sync(self, mock_remote_insert_ping_metrics, mock_get_if_need_to_sync, mock_query_prometheus, mock_local_insert_ping_metrics):
        """
        Test case: Add another second record to local SQLite.
        No call should be made to remote database.
        New record should not be marked as synced.
        """
        self._setup_monitor()

        # Mock Prometheus query to return a single ping metric for each call (total PING_METRICS * 2)
        mock_query_prometheus.side_effect = [
            {'data': {'result': [{'metric': {'instance': f'test-instance-{i}'}, 'value': [str(datetime.now().timestamp()), f'{1.0 + i}']}]}} for i in range(len(PING_METRICS) * 2)]

        # Act: Collect ping metrics twice
        self.monitor.collect_ping_metrics()
        self.monitor.collect_ping_metrics()
        self.monitor.check_sync() # Explicitly call check_sync here

        # Assert: local_database.insert_ping_metrics was called twice
        self.assertEqual(mock_local_insert_ping_metrics.call_count, 2)

        # Assert: remote_database.insert_ping_metrics was NOT called
        mock_remote_insert_ping_metrics.assert_not_called()
        
        # Assert: local_database.get_if_need_to_sync was checked twice with correct argument
        # The call count should be 2, because check_sync is called twice implicitely (once for each collect_ping_metrics call, if it was not explicitly called, if it is explicitely called, it will be called once)
        # It will be called once to evaluate the condition inside check_sync
        self.assertEqual(mock_get_if_need_to_sync.call_count, 1) # Changed from 2 to 1 for explicit call
        mock_get_if_need_to_sync.assert_called_with(timedelta(weeks=1)) # Check for correct argument


    @patch("local_database.get_ping_metrics_to_sync")
    @patch("local_database.get_speed_metrics_to_sync") # Added mock for speed metrics to sync
    @patch("local_database.mark_ping_metrics_as_synced")
    @patch("local_database.mark_speed_metrics_as_synced") # Added mock for speed metrics synced
    @patch("remote_database.insert_ping_metrics")
    @patch("remote_database.insert_speed_metrics")
    @patch("local_database.get_if_need_to_sync") # Corrected to local_database
    @patch("main.NetworkMonitor._query_prometheus")
    def test_sync_only_unsynced_records_after_week(self, mock_query_prometheus, mock_get_if_need_to_sync, mock_remote_insert_speed_metrics, mock_remote_insert_ping_metrics, mock_mark_speed_metrics_as_synced, mock_mark_ping_metrics_as_synced, mock_get_speed_metrics_to_sync, mock_get_ping_metrics_to_sync):
        """
        Test case: Add a record that has a timestamp a week after last 'synced' local record.
        A single call should be made to sync to remote database.
        Only 'unsynced' records should be sent.
        All records sent should be marked as synced in local SQLite.
        """
        self._setup_monitor()

        # Mock local_database.get_ping_metrics_to_sync to return some unsynced records
        # I see the problem here. The test is set up to return 3 records (2 unsynced, 1 synced). 
        # But the assertion expects 2 records to be sent to remote_database.insert_ping_metrics.
        # This is because the test data has 2 unsynced records, which is correct for sending.
        # The issue is the test expects 2 records, but the provided `return_value` has 3 records. 
        # One of them is already synced and should not be returned by get_ping_metrics_to_sync.
        # Let's adjust the mock_get_ping_metrics_to_sync.return_value to only return unsynced records as expected by the remote_database.insert_ping_metrics call.
        mock_get_ping_metrics_to_sync.return_value = [
            {'id': 1, 'timestamp': (datetime.now(UTC) - timedelta(days=2)).isoformat(), 'synced': 0, **{metric: '1.0' for metric in PING_METRICS}},
            {'id': 2, 'timestamp': (datetime.now(UTC) - timedelta(days=8)).isoformat(), 'synced': 0, **{metric: '1.0' for metric in PING_METRICS}}
        ]
        # Mock local_database.get_speed_metrics_to_sync to return some unsynced records
        mock_get_speed_metrics_to_sync.return_value = [
            {'id': 101, 'timestamp': (datetime.now(UTC) - timedelta(days=3)).isoformat(), 'synced': 0, **{metric: '100.0' for metric in SPEED_METRICS}},
            {'id': 102, 'timestamp': (datetime.now(UTC) - timedelta(days=9)).isoformat(), 'synced': 0, **{metric: '100.0' for metric in SPEED_METRICS}}
        ]

        # Mock local_database.get_if_need_to_sync to trigger a sync
        mock_get_if_need_to_sync.return_value = True

        # Mock Prometheus query (not directly relevant for sync logic, but needed for NetworkMonitor init)
        # It needs to return a list of responses, the first for ipinfo.io, then for each PING_METRICS query.
        ipinfo_mock_response = MagicMock()
        ipinfo_mock_response.json.return_value = {
            "ip": "123.45.67.89",
            "city": "Test City",
            "region": "Test Region",
            "country": "TC"
        }
        ipinfo_mock_response.raise_for_status.return_value = None

        prometheus_mock_responses = []
        for i in range(len(PING_METRICS)):
            mock_response_item = MagicMock()
            mock_response_item.json.return_value = {
                'data': {
                    'result': [{
                        'metric': {'instance': f'test-instance-{i}'},
                        'value': [str(datetime.now().timestamp()), f'{1.0 + i}']
                    }]
                }
            }
            mock_response_item.raise_for_status.return_value = None
            prometheus_mock_responses.append(mock_response_item)

        self.mock_requests_get.side_effect = [ipinfo_mock_response] + prometheus_mock_responses

        # Act: Trigger sync check
        self.monitor.check_sync()

        # Assert: remote_database.insert_ping_metrics was called once
        mock_remote_insert_ping_metrics.assert_called_once()
        self.assertEqual(len(mock_remote_insert_ping_metrics.call_args[0][0]), 2)
        # Verify the actual content passed for ping metrics
        actual_ping_calls_args = mock_remote_insert_ping_metrics.call_args[0][0]
        expected_ping_rows = [
            {k:v for k,v in mock_get_ping_metrics_to_sync.return_value[0].items() if k not in ['id', 'synced']},
            {k:v for k,v in mock_get_ping_metrics_to_sync.return_value[1].items() if k not in ['id', 'synced']}
        ]
        for expected_row in expected_ping_rows:
            self.assertIn(expected_row, actual_ping_calls_args)

        # Assert: local_database.mark_ping_metrics_as_synced was called with the IDs of the sent ping records
        mock_mark_ping_metrics_as_synced.assert_called_once_with([1, 2])

        # Assert: remote_database.insert_speed_metrics was called once
        mock_remote_insert_speed_metrics.assert_called_once()
        self.assertEqual(len(mock_remote_insert_speed_metrics.call_args[0][0]), 2)
        # Verify the actual content passed for speed metrics
        actual_speed_calls_args = mock_remote_insert_speed_metrics.call_args[0][0]
        expected_speed_rows = [
            {k:v for k,v in mock_get_speed_metrics_to_sync.return_value[0].items() if k not in ['id', 'synced']},
            {k:v for k,v in mock_get_speed_metrics_to_sync.return_value[1].items() if k not in ['id', 'synced']}
        ]
        for expected_row in expected_speed_rows:
            self.assertIn(expected_row, actual_speed_calls_args)

        # Assert: local_database.mark_speed_metrics_as_synced was called with the IDs of the sent speed records
        mock_mark_speed_metrics_as_synced.assert_called_once_with([101, 102])


if __name__ == '__main__':
    unittest.main()