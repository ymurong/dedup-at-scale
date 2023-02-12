#!/bin/bash

docker build -f ./docker/Dockerfile.dedup-api -t  dedup-api:1.0.0  .
docker compose -f ./docker/docker-compose.yml up