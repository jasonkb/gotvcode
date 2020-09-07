#!/usr/bin/env bash
set -euo pipefail

# Use a different port and docker-compose project name to allow tests to be run
# concurrently with the dev server
export DYNAMODB_URL=http://localhost:8001

export TOES_TESTING=true

docker-compose -f test-docker-compose.yml -p toes_test up --remove-orphans -d

cleanup() {
  docker-compose -f test-docker-compose.yml -p toes_test down
}
trap cleanup EXIT

pipenv run python -m pytest -vv "$@"
