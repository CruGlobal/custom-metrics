import unittest
from unittest.mock import patch, MagicMock, mock_open, AsyncMock
import os
import sys
import uuid
import requests
import asyncio

# Adjust the path to import modules from the parent directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main import NetworkMonitor, PING_METRICS, SPEED_METRICS, main # Import main function

class TestNetworkMonitor(unittest.TestCase):

    def setUp(self):
        # Ensure a clean state for environment variables and device_id file
        self.original_device_id_env = os.getenv("DEVICE_ID")
        self.original_prometheus_url_env = os.getenv("PROMETHEUS_URL")

        os.environ["DEVICE_ID"] = "TestSiteID"
        os.environ["PROMETHEUS_URL"] = "http://mock-prometheus:9090"

        # Clean up any existing device_id file before each test
        if os.path.exists("network-monitor/device_id"):
            os.remove("network-monitor/device_id")
        if os.path.exists("network-monitor"):
            os.rmdir("network-monitor")

    def tearDown(self):
        if self.original_device_id_env is not None:
            os.environ["DEVICE_ID"] = self.original_device_id_env
        else:
            if "DEVICE_ID" in os.environ:
                del os.environ["DEVICE_ID"]

        if self.original_prometheus_url_env is not None:
            os.environ["PROMETHEUS_URL"] = self.original_prometheus_url_env
        else:
            if "PROMETHEUS_URL" in os.environ:
                del os.environ["PROMETHEUS_URL"]

        # Clean up any created device_id file after each test
        if os.path.exists("network-monitor/device_id"):
            os.remove("network-monitor/device_id")
        if os.path.exists("network-monitor"):
            os.rmdir("network-monitor")

    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open, read_data="existing_device_id")
    @patch.object(NetworkMonitor, '__init__', return_value=None) # Mock __init__ to prevent side effects
    def test_get_or_create_device_id_exists(self, mock_init, mock_file, mock_exists):
        mock_exists.return_value = True
        monitor = NetworkMonitor()
        monitor.device_id = "mock_device_id" # Manually set device_id as __init__ is mocked
        device_id = monitor._get_or_create_device_id()
        self.assertEqual(device_id, "existing_device_id")
        mock_exists.assert_called_once_with("network-monitor/device_id")
        mock_file.assert_called_once_with("network-monitor/device_id", 'r')

    @patch('os.path.exists')
    @patch('os.makedirs')
    @patch('builtins.open', new_callable=mock_open)
    @patch('uuid.uuid4', return_value=uuid.UUID('12345678-1234-5678-1234-567812345678'))
    @patch.object(NetworkMonitor, '__init__', return_value=None) # Mock __init__ to prevent side effects
    def test_get_or_create_device_id_creates_new(self, mock_init, mock_uuid, mock_file, mock_makedirs, mock_exists):
        mock_exists.return_value = False
        monitor = NetworkMonitor()
        monitor.device_id = "mock_device_id" # Manually set device_id as __init__ is mocked
        device_id = monitor._get_or_create_device_id()
        self.assertEqual(device_id, "12345678-1234-5678-1234-567812345678")
        mock_makedirs.assert_called_once_with("network-monitor", exist_ok=True)
        mock_file.assert_called_once_with("network-monitor/device_id", 'w')
        mock_file().write.assert_called_once_with("12345678-1234-5678-1234-567812345678")

    @patch('requests.get')
    def test_query_prometheus_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": {"result": [{"value": [0, "1"]}]}}
        mock_get.return_value = mock_response

        monitor = NetworkMonitor()
        result = monitor._query_prometheus("test_query")
        self.assertEqual(result, {"data": {"result": [{"value": [0, "1"]}]}})
        mock_get.assert_called_once_with("http://mock-prometheus:9090/api/v1/query", params={"query": "test_query"})

    @patch('requests.get', side_effect=requests.exceptions.RequestException("Prometheus Error"))
    def test_query_prometheus_failure(self, mock_get):
        monitor = NetworkMonitor()
        result = monitor._query_prometheus("test_query")
        self.assertIsNone(result)

    @patch('main.ping') # Correct patch target
    def test_insert_ping_metrics(self, mock_ping):
        monitor = NetworkMonitor()
        monitor.device_id = "test_device_id"
        metrics_data = {"metric1": 1, "metric2": 0.5}
        monitor._insert_ping_metrics(metrics_data)

        expected_metrics_data = {
            "metric1": 1,
            "metric2": 0.5,
            "device_id": "test_device_id",
        }
        mock_ping.assert_called_once_with(expected_metrics_data)

    @patch('main.speed') # Correct patch target
    def test_insert_speed_metrics(self, mock_speed):
        monitor = NetworkMonitor()
        monitor.device_id = "test_device_id"
        metrics_data = {"download_mbps": 100, "upload_mbps": 50}
        monitor._insert_speed_metrics(metrics_data)

        expected_metrics_data = {
            "download_mbps": 100,
            "upload_mbps": 50,
            "device_id": "test_device_id",
        }
        mock_speed.assert_called_once_with(expected_metrics_data)

    @patch.object(NetworkMonitor, '_query_prometheus')
    @patch.object(NetworkMonitor, '_insert_ping_metrics')
    def test_collect_ping_metrics(self, mock_insert_ping_metrics, mock_query_prometheus):
        # Mock Prometheus responses for ping metrics (14 metrics in PING_METRICS)
        mock_query_prometheus.side_effect = [
            {"data": {"result": [{"value": [0, "1"]}]}},  # google_up
            {"data": {"result": [{"value": [0, "1"]}]}},  # apple_up
            {"data": {"result": [{"value": [0, "1"]}]}},  # github_up
            {"data": {"result": [{"value": [0, "1"]}]}},  # windowsupdate_up
            {"data": {"result": [{"value": [0, "1"]}]}},  # signon_okta_up
            {"data": {"result": [{"value": [0, "1"]}]}},  # pihole_up
            {"data": {"result": [{"value": [0, "1"]}]}},  # node_up
            {"data": {"result": [{"value": [0, "1"]}]}},  # speedtest_up
            {"data": {"result": [{"value": [0, "0.123"]}]}},  # http_latency
            {"data": {"result": [{"value": [0, "10"]}]}},  # http_samples
            {"data": {"result": [{"value": [0, "0.5"]}]}},  # http_time
            {"data": {"result": [{"value": [0, "100"]}]}},  # http_content_length
            {"data": {"result": [{"value": [0, "0.2"]}]}},  # http_duration
            {"data": {"result": [{"value": [0, "3600"]}]}},  # uptime
        ]
        monitor = NetworkMonitor()
        monitor.device_id = "test_device_id"
        monitor.collect_ping_metrics()

        expected_metrics = {
            "google_up": 1,
            "http_latency": 0.123,
            "github_up": 1,
            "http_content_length": 100.0
        }
        mock_insert_ping_metrics.assert_called_once()
        # Check if the collected metrics are a subset of the expected metrics
        # and that the 'up' metrics are correctly converted to int
        actual_metrics_called = mock_insert_ping_metrics.call_args[0][0]
        for key, value in expected_metrics.items():
            self.assertIn(key, actual_metrics_called)
            self.assertEqual(actual_metrics_called[key], value)

    @patch.object(NetworkMonitor, '_query_prometheus')
    @patch.object(NetworkMonitor, '_insert_speed_metrics')
    def test_collect_speed_metrics(self, mock_insert_speed_metrics, mock_query_prometheus):
        # Mock Prometheus responses for speed metrics
        mock_query_prometheus.side_effect = [
            {"data": {"result": [{"value": [0, "100000000"]}]}}, # download_mbps (initial check)
            {"data": {"result": [{"value": [0, "100000000"]}]}}, # download_mbps
            {"data": {"result": [{"value": [0, "50000000"]}]}},  # upload_mbps
            {"data": {"result": [{"value": [0, "25.5"]}]}},      # ping_ms
            {"data": {"result": [{"value": [0, "5.1"]}]}},       # jitter_ms
            {"data": {"result": [{"value": [0, "86400.0"]}]}},   # uptime (seconds)
        ]

        monitor = NetworkMonitor()
        monitor.device_id = "test_device_id"
        monitor.collect_speed_metrics()

        expected_metrics = {
            "device_id": "test_device_id",
            "download_mbps": 100.0, # Converted from bits to Mbps
            "upload_mbps": 50.0,    # Converted from bits to Mbps
            "ping_ms": 25.5,
            "jitter_ms": 5.1,
            "uptime": 86400.0,
        }
        mock_insert_speed_metrics.assert_called_once_with(expected_metrics)

    @patch.object(NetworkMonitor, '_query_prometheus', return_value=None)
    @patch.object(NetworkMonitor, '_insert_speed_metrics')
    def test_collect_speed_metrics_no_data(self, mock_insert_speed_metrics, mock_query_prometheus):
        monitor = NetworkMonitor()
        monitor.collect_speed_metrics()
        mock_insert_speed_metrics.assert_not_called()

    @patch('main.NetworkMonitor')
    @patch('schedule.every')
    @patch('asyncio.sleep', new_callable=AsyncMock) # Use AsyncMock for awaitable
    @patch('schedule.run_pending')
    def test_main_function(self, mock_run_pending, mock_async_sleep, mock_schedule_every, mock_network_monitor_class):
        # Mock the NetworkMonitor instance and its methods
        mock_monitor_instance = MagicMock()
        mock_network_monitor_class.return_value = mock_monitor_instance

        # Mock asyncio.sleep to break the while loop after a few iterations
        mock_async_sleep.side_effect = [asyncio.sleep(0.01), asyncio.sleep(0.01), asyncio.CancelledError]

        # Call the main function
        with self.assertRaises(asyncio.CancelledError):
            asyncio.run(main())

        # Assertions
        mock_network_monitor_class.assert_called_once()
        mock_monitor_instance.collect_ping_metrics.assert_called() # Called twice: once initially, once by schedule
        mock_monitor_instance.collect_speed_metrics.assert_called() # Called twice: once initially, once by schedule

        # Check scheduling calls
        mock_schedule_every.assert_any_call(5)
        mock_schedule_every().minutes.do.assert_any_call(mock_monitor_instance.collect_ping_metrics)
        mock_schedule_every.assert_any_call(60)
        mock_schedule_every().minutes.do.assert_any_call(mock_monitor_instance.collect_speed_metrics)

        # Ensure run_pending was called
        self.assertTrue(mock_run_pending.called)


if __name__ == '__main__':
    unittest.main()
