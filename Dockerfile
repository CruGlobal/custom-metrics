# custom-metrics/Dockerfile
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
   iputils-ping \
   && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create directory for site ID
RUN mkdir -p /etc/network-monitor

# Development mode uses volume mounting
VOLUME ["/app"]

# Default to production mode
ENV ENVIRONMENT=production

# Use different commands for dev/prod
CMD if [ "$ENVIRONMENT" = "development" ]; then \
   python main.py; \
   else \
   while true; do python main.py; sleep 300; done; \
   fi

