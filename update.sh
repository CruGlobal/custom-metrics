#!/bin/bash

docker exec -it bigquery-metrics rm /app/main.py
docker container cp ./main.py bigquery-metrics:/app/
