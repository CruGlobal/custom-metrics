import unittest
import sqlite3
import os
from datetime import datetime, timedelta, UTC
from unittest import mock # Import mock explicitly
import local_database

class TestLocalDatabase(unittest.TestCase):
    def setUp(self):
        """Set up a temporary SQLite database file for testing."""
        self.db_file = "test_metrics.db" # Use a temporary file for the database
        local_database.DB_FILE = self.db_file # Point local_database to the test db file
        local_database.init_db()

        # Patch datetime.now to return a fixed time for consistent testing
        self.fixed_now = datetime(2026, 2, 12, 16, 0, 0, tzinfo=UTC)
        
        # Patch datetime.now in local_database
        self.datetime_patcher = mock.patch('local_database.datetime')
        self.mock_datetime = self.datetime_patcher.start()
        self.mock_datetime.now.return_value = self.fixed_now
        self.mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw) # Allow other datetime calls to work

    def tearDown(self):
        """Clean up after each test."""
        # Restore original DB_FILE and remove the test database file
        local_database.DB_FILE = "metrics.db"
        self.datetime_patcher.stop()
        if os.path.exists(self.db_file):
            os.remove(self.db_file)

    def test_insert_ping_metrics_bug_reproduction(self):
        """
        Test to reproduce the '13 values for 14 columns' error.
        This test should initially fail with sqlite3.ProgrammingError.
        """
        # Prepare metrics data with 13 values (missing http_duration)
        metrics_data = {
            "site_id": "test-site",
            "location": "test-location",
            "google_up": 1,
            "apple_up": 1,
            "github_up": 1,
            "pihole_up": 1,
            "node_up": 1,
            "speedtest_up": 1,
            "http_latency": 0.1,
            "http_samples": 10,
            "http_time": 0.5,
            "http_content_length": 100
            # http_duration is intentionally missing to reproduce the bug
        }

        # This should no longer raise an error as the bug is fixed
        try:
            local_database.insert_ping_metrics(metrics_data)
        except sqlite3.ProgrammingError as e:
            self.fail(f"insert_ping_metrics raised ProgrammingError unexpectedly: {e}")

        # Verify the data was inserted
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM ping_metrics WHERE site_id = 'test-site'")
        row = cursor.fetchone()
        conn.close()

        self.assertIsNotNone(row)
        self.assertEqual(row[2], "test-site") # site_id
        self.assertEqual(row[3], "test-location") # location
        # Check http_duration, which was the missing value
        self.assertIsNone(row[14]) # http_duration should be None as it was not provided

    def test_insert_ping_metrics_success_after_fix(self):
        """
        Test successful insertion of ping metrics with all 14 columns.
        This test should pass after the fix is applied.
        """
        metrics_data = {
            "site_id": "test-site",
            "location": "test-location",
            "google_up": 1,
            "apple_up": 1,
            "github_up": 1,
            "pihole_up": 1,
            "node_up": 1,
            "speedtest_up": 1,
            "http_latency": 0.1,
            "http_samples": 10,
            "http_time": 0.5,
            "http_content_length": 100,
            "http_duration": 0.2 # All 14 values present
        }

        # This should not raise an error after the fix
        try:
            local_database.insert_ping_metrics(metrics_data)
        except sqlite3.ProgrammingError as e:
            self.fail(f"insert_ping_metrics raised ProgrammingError unexpectedly: {e}")

        # Verify the data was inserted
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM ping_metrics WHERE site_id = 'test-site'")
        row = cursor.fetchone()
        conn.close()

        self.assertIsNotNone(row)
        self.assertEqual(row[2], "test-site") # site_id
        self.assertEqual(row[3], "test-location") # location
        self.assertEqual(row[14], 0.2) # http_duration

    def test_insert_ping_metrics_with_provided_timestamp(self):
        """
        Test that a provided timestamp is correctly inserted for ping metrics.
        """
        provided_timestamp = datetime(2025, 1, 1, 10, 30, 0, tzinfo=UTC).isoformat()
        metrics_data = {
            "timestamp": provided_timestamp,
            "site_id": "test-site-ts",
            "location": "test-location-ts",
            "google_up": 1, "apple_up": 1, "github_up": 1, "pihole_up": 1,
            "node_up": 1, "speedtest_up": 1, "http_latency": 0.1,
            "http_samples": 10, "http_time": 0.5, "http_content_length": 100,
            "http_duration": 0.2
        }
        local_database.insert_ping_metrics(metrics_data)

        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT timestamp FROM ping_metrics WHERE site_id = 'test-site-ts'")
        row = cursor.fetchone()
        conn.close()

        self.assertIsNotNone(row)
        self.assertEqual(row[0], provided_timestamp)

    def test_insert_ping_metrics_without_timestamp(self):
        """
        Test that the default timestamp is used when none is provided for ping metrics.
        """
        metrics_data = {
            "site_id": "test-site-no-ts",
            "location": "test-location-no-ts",
            "google_up": 1, "apple_up": 1, "github_up": 1, "pihole_up": 1,
            "node_up": 1, "speedtest_up": 1, "http_latency": 0.1,
            "http_samples": 10, "http_time": 0.5, "http_content_length": 100,
            "http_duration": 0.2
        }
        local_database.insert_ping_metrics(metrics_data)

        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT timestamp FROM ping_metrics WHERE site_id = 'test-site-no-ts'")
        row = cursor.fetchone()
        conn.close()

        self.assertIsNotNone(row)
        self.assertEqual(row[0], self.fixed_now.isoformat())

    def test_insert_speed_metrics_with_provided_timestamp(self):
        """
        Test that a provided timestamp is correctly inserted for speed metrics.
        """
        provided_timestamp = datetime(2025, 1, 2, 11, 0, 0, tzinfo=UTC).isoformat()
        metrics_data = {
            "timestamp": provided_timestamp,
            "site_id": "test-site-speed-ts",
            "location": "test-location-speed-ts",
            "download_mbps": 100.0,
            "upload_mbps": 50.0,
            "ping_ms": 10.0,
            "jitter_ms": 2.0
        }
        local_database.insert_speed_metrics(metrics_data)

        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT timestamp FROM speed_metrics WHERE site_id = 'test-site-speed-ts'")
        row = cursor.fetchone()
        conn.close()

        self.assertIsNotNone(row)
        self.assertEqual(row[0], provided_timestamp)

    def test_insert_speed_metrics_without_timestamp(self):
        """
        Test that 'now' is default timestamp is used when none is provided for speed metrics.
        """
        metrics_data = {
            "site_id": "test-site-speed-no-ts",
            "location": "test-location-speed-no-ts",
            "download_mbps": 100.0,
            "upload_mbps": 50.0,
            "ping_ms": 10.0,
            "jitter_ms": 2.0
        }
        local_database.insert_speed_metrics(metrics_data)

        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        when_made = datetime.now(UTC).isoformat()
        cursor.execute("SELECT timestamp FROM speed_metrics WHERE site_id = 'test-site-speed-no-ts'")
        row = cursor.fetchone()
        conn.close()

        self.assertIsNotNone(row)
        self.assertEqual(row[0], when_made)

if __name__ == '__main__':
    unittest.main()
