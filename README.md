# Network Monitoring Service

A Python service that collects network metrics from Prometheus and stores them in google sheets. The service monitors various network metrics including ping times to major services (Google, Apple, GitHub) and speedtest results.

## Setup

1. Create a Python virtual environment:

```bash
# Create a new virtual environment
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
bash ./pi_rebuild.sh
```

Create a `.env` file in the project root with the following variables
```bash
DEVICE_ID=your-site-id
```

## License

This project is licensed under the MIT License - see the LICENSE file for details. 