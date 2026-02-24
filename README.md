# Network Monitoring Service

A Python service that collects network metrics from Prometheus and stores them in google sheets. The service monitors various network metrics including ping times to major services (Google, Apple, GitHub) and speedtest results.

## Setup

1. Create a Python virtual environment:

```bash
# Create a new virtual environment
python3 -m venv venv
# Activate the virtual environment On macOS/Linux:
source venv/bin/activate
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

```bash
python3 main.py
```

```bash
# Run all tests
python3 -m unittest test_network_monitor.py -v
```

## License

This project is licensed under the MIT License - see the LICENSE file for details. 