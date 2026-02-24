#!bin/bash

docker build --platform linux/arm/v8 -t custom-metrics:local --build-arg BUILDPLATFORM=linux/arm/v8 --build-arg TARGETPLATFORM=linux/arm/v8 .