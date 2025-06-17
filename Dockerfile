# custom-metrics/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Create config directory for credentials
RUN mkdir -p /app/config

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .

# Set environment variables
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/config/credentials.json

# Run the application
CMD ["python", "main.py"]

