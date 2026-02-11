# Network Monitoring Service

A Python service that collects network metrics from Prometheus and stores them in BigQuery for analysis. The service monitors various network metrics including ping times to major services (Google, Apple, GitHub) and speedtest results.

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

```bash
pip3 install -r requirements.txt
```
GOOGLE_APPLICATION_CREDENTIALS=./.secret.json python3 test_connection.py

Create a `.env` file in the project root with the following variables

```bash
BIGQUERY_PROJECT=your-project-id
BIGQUERY_DATASET=internet_monitoring1
PING_TABLE=ping
SPEED_TABLE=speed
PROMETHEUS_URL=http://your-prometheus:9090
LOCATION=your-location
```

## Running the Service

```bash
python3 main.py
```
## Running Tests

```bash
# Run all tests
python3 -m unittest test_network_monitor.py -v
```

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