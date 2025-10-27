#!/bin/bash

docker build -f Dockerfile.layer -t pseudonymisation-layer-builder .
docker create --name temp-layer pseudonymisation-layer-builder
docker cp temp-layer:/layer.zip ./pseudonymisation-layer.zip
docker rm temp-layer

echo "Layer built: pseudonymisation-layer.zip"
