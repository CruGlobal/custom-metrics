#!bin/bash
# this updates the stack with changes 

bash ./build_local.sh
yq eval 'del(.services.custom-metrics.pull_policy)' /scry-pi/internet-monitoring/docker-compose.yml -i
cd /scry-pi/internet-monitoring/
docker compose up -d 

sleep 10
docker logs internet-monitoring-custom-metrics-1
