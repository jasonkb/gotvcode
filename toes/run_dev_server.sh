#!/usr/bin/env bash
set -euo pipefail

docker-compose up -d

cleanup() {
  docker-compose down
}
trap cleanup EXIT

export FLASK_APP=$1
export FLASK_DEBUG=1
export STAGE=dev
export TELEMETRY_DISABLE=1
pipenv run python -m flask run
