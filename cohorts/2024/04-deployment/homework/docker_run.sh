#!/bin/bash

DOCKER_IMAGE_NAME="jelambrar96/zoomcamp-model:mlops-2024-3.10.13-slim"

docker build  -t $DOCKER_IMAGE_NAME .
docker run --rm  -v $(pwd)/outputs:/app/outputs $DOCKER_IMAGE_NAME --year 2023 --month 5

