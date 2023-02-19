#!/bin/bash
echo "> Defining BASEDIR (SCRIPT DIR LOCATION)"
__BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
if [ -z "$__BASEDIR" ]; then
  echo "__BASEDIR: undefined"
  exit 1
fi
docker compose -f "$__BASEDIR"/docker/docker-compose.yml down
docker rmi dedup-api:1.0.0