#!/bin/bash
########################################################
# THIS IS USED TO KILL A LOCAL STANDALONE SPARK CLUSTER
########################################################
echo "> Defining BASEDIR (SCRIPT DIR LOCATION)"
__BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
if [ -z "$__BASEDIR" ]; then
  echo "__BASEDIR: undefined"
  exit 1
fi

docker compose -f "$__BASEDIR"/docker-compose-only-spark.yml down