# Network Monitoring Service

A Python service that collects network metrics from Prometheus and stores them in BigQuery for analysis. The service monitors various network metrics including ping times to major services (Google, Apple, GitHub) and speedtest results.

## Features

- Automatic site ID generation and management
- Prometheus metric collection
- BigQuery data storage with separate tables for ping and speed metrics
- Configurable metrics collection interval
- Comprehensive logging
- Ping monitoring for major services
- Speedtest monitoring (download, upload, ping, jitter)

## Prerequisites

- Python 3.8 or higher
- Google Cloud Platform account with BigQuery access
- Prometheus instance running and accessible
- Service account credentials for BigQuery access
- Speedtest exporter running and accessible

## Setup

1. Create a Python virtual environment:

```bash
# Create a new virtual environment
python3 -m venv venv

# Activate the virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
.\venv\Scripts\activate
```

2. Install dependencies:

```bash
pip3 install -r requirements.txt
```

3. Set up environment variables:

Create a `.env` file in the project root with the following variables:

```bash
BIGQUERY_PROJECT=your-project-id
BIGQUERY_DATASET=internet_monitoring1
PING_TABLE=ping
SPEED_TABLE=speed
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
python3 main.py
```

The service will:
- Generate or retrieve a site ID
- Connect to BigQuery
- Start collecting ping metrics every 5 minutes
- Check for and store speedtest metrics when available
- Log all activities to the console

## Running Tests

Run the test suite:

```bash
# Run all tests
python3 -m unittest test_network_monitor.py -v

# Run a specific test
python3 -m unittest test_network_monitor.TestNetworkMonitor.test_query_prometheus_success -v
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

### Ping Metrics (collected every 5 minutes)
- Service status (up/down) for:
  - Google
  - Apple
  - GitHub
  - PiHole
  - Node Exporter
  - Speedtest
- HTTP metrics:
  - Latency
  - Samples
  - Time
  - Content length
  - Duration

### Speedtest Metrics (collected when available)
- Download speed (Mbps)
- Upload speed (Mbps)
- Ping latency (ms)
- Jitter (ms)

## BigQuery Schema

### Ping Table Schema
```sql
site_id:STRING,
timestamp:TIMESTAMP,
location:STRING,
google_up:FLOAT,
apple_up:FLOAT,
github_up:FLOAT,
pihole_up:FLOAT,
node_up:FLOAT,
speedtest_up:FLOAT,
http_latency:FLOAT,
http_samples:FLOAT,
http_time:FLOAT,
http_content_length:FLOAT,
http_duration:FLOAT
```

### Speed Table Schema
```sql
site_id:STRING,
timestamp:TIMESTAMP,
location:STRING,
download_mbps:FLOAT,
upload_mbps:FLOAT,
ping_ms:FLOAT,
jitter_ms:FLOAT
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 