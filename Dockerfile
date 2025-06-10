# custom-metrics/Dockerfile
FROM python:3.11-slim
COPY . /app
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# CMD ["python", "main.py"]
CMD ["sh", "-c", "while true; do python main.py; sleep 300; done"]

