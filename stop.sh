#!/bin/bash

docker compose -f ./docker/docker-compose.yml down
docker rmi dedup-api:1.0.0