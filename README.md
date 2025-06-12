# Network Monitoring Service

A Python service that collects network metrics from Prometheus and stores them in BigQuery for analysis. The service monitors various network metrics including ping times to major services (Google, Apple, GitHub) and system-level network statistics.

## Features

- Automatic site ID generation and management
- Prometheus metric collection
- BigQuery data storage
- Configurable metrics collection interval
- Comprehensive logging
- Ping monitoring for major services

## Prerequisites

- Python 3.8 or higher
- Google Cloud Platform account with BigQuery access
- Prometheus instance running and accessible
- Service account credentials for BigQuery access

## Setup

1. Create a Python virtual environment:

```bash
# Create a new virtual environment
python -m venv venv

# Activate the virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
.\venv\Scripts\activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Set up environment variables:

Create a `.env` file in the project root with the following variables:

```bash
BIGQUERY_PROJECT=your-project-id
PROMETHEUS_URL=http://your-prometheus:9090
LOCATION=your-location
```

4. Set up Google Cloud credentials:

Place your Google Cloud service account credentials JSON file in a secure location and set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/credentials.json"
```

## Running the Service

Start the service:

```bash
python main.py
```

The service will:
- Generate or retrieve a site ID
- Connect to BigQuery
- Start collecting metrics every 5 minutes
- Log all activities to the console

## Running Tests

Run the test suite:

```bash
# Run all tests
python -m unittest test_network_monitor.py -v

# Run a specific test
python -m unittest test_network_monitor.TestNetworkMonitor.test_query_prometheus_success -v
```

## Project Structure

```
.
├── main.py              # Main service implementation
├── test_network_monitor.py  # Test suite
├── requirements.txt     # Python dependencies
├── .env                # Environment variables (create this)
└── README.md           # This file
```

## Metrics Collected

The service collects the following metrics:
- `scrape_duration_seconds`
- `scrape_samples_scraped`
- `scrape_series_added`
- `node_netstat_TcpExt_SyncookiesFailed`
- Ping metrics for Google, Apple, and GitHub services

## BigQuery Schema

The data is stored in BigQuery with the following schema:
- `timestamp`: ISO format timestamp
- `site_id`: Unique identifier for the monitoring site
- `metric`: Name of the metric
- `job`: Job identifier
- `location`: Location identifier
- `value`: Metric value
- `instance`: Instance identifier
- `google_ping`: Google ping status
- `google_time`: Google ping time
- `google_samples`: Google ping samples
- `apple_ping`: Apple ping status
- `apple_time`: Apple ping time
- `apple_samples`: Apple ping samples
- `github_ping`: GitHub ping status
- `github_time`: GitHub ping time
- `github_samples`: GitHub ping samples

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 