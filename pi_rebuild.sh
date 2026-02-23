#!bin/bash
# this updates the stack with changes 

bash ./build_local.sh
cd /scry-pi/internet-monitoring/
docker compose up -d 

docker logs internet-monitoring-custom-metrics-1 