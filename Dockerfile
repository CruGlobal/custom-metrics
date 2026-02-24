# custom-metrics/Dockerfile
FROM --platform=linux/arm64/v8 python:3.13-slim-bookworm

WORKDIR /app

# Install system dependencies required for building some Python packages
RUN apt-get update && \
   apt-get install -y --no-install-recommends \
   && \
   rm -rf /var/lib/apt/lists/*

# Create config directory for credentials
RUN mkdir -p /app/config

# Install dependencies
RUN pip install --no-cache-dir --upgrade pip setuptools
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .
COPY submit_to_google_form.py .

# Run the application
CMD ["python", "main.py"]
