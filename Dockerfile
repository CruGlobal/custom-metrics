# custom-metrics/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies required for building some Python packages
RUN apt-get update && \
   apt-get install -y --no-install-recommends \
   build-essential \
   python3-dev && \
   rm -rf /var/lib/apt/lists/*

# Create config directory for credentials
RUN mkdir -p /app/config

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .
COPY database.py .

# Run the application
CMD ["python", "main.py"]
