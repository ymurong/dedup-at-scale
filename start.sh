#!/bin/bash
echo "> Defining BASEDIR (SCRIPT DIR LOCATION)"
__BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
if [ -z "$__BASEDIR" ]; then
  echo "__BASEDIR: undefined"
  exit 1
fi
docker build -f "$__BASEDIR"/docker/Dockerfile.dedup-api -t  dedup-api:1.0.0  .
docker compose -f "$__BASEDIR"/docker/docker-compose.yml up -d