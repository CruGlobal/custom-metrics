import unittest
from unittest.mock import patch, MagicMock
import os
import sys
import datetime
import requests
import time # Import time for sleep mock

# Adjust the path to import modules from the parent directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from submit_to_google_form import format_data, _send_form_request, ping, speed, PING_FORM_URL, SPEED_FORM_URL, PING_FORM_ENTRY_IDS, SPEED_FORM_ENTRY_IDS

class TestGoogleForm(unittest.TestCase):

    @patch('submit_to_google_form._send_form_request')
    def test_format_data_ping(self, mock_send_form_request):
        metrics_data = {
            "device_id": "test_device_id",
            "location": "test_location",
            "ip_address": "127.0.0.1",
            "netsuite_up": 1,
            "http_latency": 0.05
        }
        
        with patch('datetime.datetime') as mock_datetime:
            mock_now = MagicMock()
            mock_now.strftime.return_value = "2026-03-04 10:30:00"
            mock_datetime.now.return_value = mock_now
            mock_datetime.side_effect = lambda *args, **kw: datetime.datetime(*args, **kw) # Keep this for other potential datetime calls

            format_data(metrics_data, PING_FORM_URL, PING_FORM_ENTRY_IDS)

            expected_form_data = {
                PING_FORM_ENTRY_IDS["local_timestamp"]: "2026-03-04 10:30:00",
                PING_FORM_ENTRY_IDS["device_id"]: "test_device_id",
                PING_FORM_ENTRY_IDS["location"]: "test_location",
                PING_FORM_ENTRY_IDS["ip_address"]: "127.0.0.1",
                PING_FORM_ENTRY_IDS["netsuite_up"]: "1",
                PING_FORM_ENTRY_IDS["http_latency"]: "0.05"
            }
            mock_send_form_request.assert_called_once_with(expected_form_data, PING_FORM_URL)

    @patch('submit_to_google_form._send_form_request')
    def test_format_data_speed(self, mock_send_form_request):
        metrics_data = {
            "device_id": "test_device_id_speed",
            "location": "test_location_speed",
            "download_mbps": 100.5,
            "upload_mbps": 50.2,
            "ip_address": "127.0.0.1",
            "uptime": 12345.0,
        }

        with patch('datetime.datetime') as mock_datetime:
            mock_now = MagicMock()
            mock_now.strftime.return_value = "2026-03-04 11:00:00"
            mock_datetime.now.return_value = mock_now
            mock_datetime.side_effect = lambda *args, **kw: datetime.datetime(*args, **kw)

            format_data(metrics_data, SPEED_FORM_URL, SPEED_FORM_ENTRY_IDS)

            expected_form_data = {
                SPEED_FORM_ENTRY_IDS["local_timestamp"]: "2026-03-04 11:00:00",
                SPEED_FORM_ENTRY_IDS["device_id"]: "test_device_id_speed",
                SPEED_FORM_ENTRY_IDS["location"]: "test_location_speed",
                SPEED_FORM_ENTRY_IDS["download_mbps"]: "100.5",
                SPEED_FORM_ENTRY_IDS["upload_mbps"]: "50.2",
                SPEED_FORM_ENTRY_IDS["ip_address"]: "127.0.0.1",
                SPEED_FORM_ENTRY_IDS["uptime"]: "12345.0",
            }
            mock_send_form_request.assert_called_once_with(expected_form_data, SPEED_FORM_URL)

    @patch('requests.post')
    @patch('time.sleep', return_value=None) # Mock time.sleep to avoid actual delays
    def test_send_form_request_success(self, mock_sleep, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        form_data = {"entry.123": "value"}
        form_url = "http://test.com/form"

        _send_form_request(form_data, form_url)
        mock_post.assert_called_once_with(form_url, data=form_data)
        mock_sleep.assert_not_called()

    @patch('requests.post')
    @patch('time.sleep', return_value=None)
    def test_send_form_request_retry_429(self, mock_sleep, mock_post):
        mock_response_success = MagicMock()
        mock_response_success.status_code = 200
        mock_response_success.raise_for_status.return_value = None

        # Simulate a 429 response for the first call, then success
        mock_response_429 = MagicMock()
        mock_response_429.status_code = 429
        mock_response_429.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response_429)

        mock_post.side_effect = [
            mock_response_429,
            mock_response_success
        ]

        form_data = {"entry.123": "value"}
        form_url = "http://test.com/form"

        _send_form_request(form_data, form_url, retries=2, delay_seconds=0) # Set delay to 0 for faster test

        self.assertEqual(mock_post.call_count, 2)
        mock_post.assert_called_with(form_url, data=form_data)
        mock_sleep.assert_called_once_with(0) # Ensure sleep was called once with the specified delay

    @patch('requests.post')
    @patch('time.sleep', return_value=None)
    def test_send_form_request_failure_non_429(self, mock_sleep, mock_post):
        # Simulate a non-429 error
        mock_response_error = MagicMock()
        mock_response_error.status_code = 500
        mock_response_error.raise_for_status.side_effect = requests.exceptions.RequestException("Internal Server Error")
        mock_post.return_value = mock_response_error

        form_data = {"entry.123": "value"}
        form_url = "http://test.com/form"

        _send_form_request(form_data, form_url, retries=3)

        mock_post.assert_called_once_with(form_url, data=form_data)
        mock_sleep.assert_not_called() # No retry for non-429 errors

    @patch('submit_to_google_form.format_data')
    def test_ping_function(self, mock_format_data):
        metrics_data = {"test_metric": 1}
        ping(metrics_data)
        mock_format_data.assert_called_once_with(metrics_data, PING_FORM_URL, PING_FORM_ENTRY_IDS)

    @patch('submit_to_google_form.format_data')
    def test_speed_function(self, mock_format_data):
        metrics_data = {"test_metric": 100.0}
        speed(metrics_data)
        mock_format_data.assert_called_once_with(metrics_data, SPEED_FORM_URL, SPEED_FORM_ENTRY_IDS)

if __name__ == '__main__':
    unittest.main()
