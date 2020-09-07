#!/usr/bin/env bash
set -euo pipefail

# Single script to deploy any zappa project in this monorepo from a Lambda-compatible
# Linux container. Deploying this way allows us to install native dependencies for
# Linux and to avoid packaging dev dependencies.
#
# Possible improvements:
#   * Better error messages for  missing positional arguments
#   * Support installing local packages from tc

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd)"
ZAPPA_BUILD_VERSION=2
COMMAND="update"

usage () {
  echo "Usage: ./zappa-deploy.sh  [-p PROJECT] [-s STAGE] [-c COMMAND]

  Required arguments:
    -p        The project to deploy, e.g. toes, supportal
    -s        The 'stage' to deploy, e.g. dev, prod

  Optional arguments:
    -c        The zappa command, defaults to 'update'
  ";
}

while getopts "c:p:s:h" opt; do
  case ${opt} in
    h ) usage; exit 0;;
    p ) PROJECT=$OPTARG;;
    s ) STAGE=$OPTARG;;
    c ) COMMAND=$OPTARG;;
    ? ) usage; exit 1;;
  esac
done

echo "Running zappa command: ${COMMAND} for project: ${PROJECT} stage: ${STAGE}"

if [ ! -d "${DIR}/${PROJECT}" ]; then
  echo "Project not found!"
  exit 1
fi

if [[ "$(docker images -q ew-zappa-build:${ZAPPA_BUILD_VERSION} 2> /dev/null)" == "" ]]; then
  echo "Installing zappa-build image locally"
  ./zappa-build/install.sh
fi

# We mount the dev directory in read-only mode to avoid clobbering the local venv
# and copy it to a read-writeable directory to pipenv install.
SCRIPT="
  rsync -av --exclude=.venv \\
     --exclude=node_modules \\
     --exclude=.serverless \\
     --exclude=venv \\
     --exclude=.git \\
     --exclude=__pycache__ \\
     --exclude=.pytest_cache \\
     --exclude=AshNazg \\
     /mnt/tc /var/task/

  cd /var/task/tc/${PROJECT}
  pipenv install
  pipenv run zappa ${COMMAND} ${STAGE}
"

docker run -ti \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SESSION_TOKEN \
  -e AWS_DEFAULT_REGION=us-east-2 \
  -v "${DIR}":/mnt/tc:ro \
  --name=zappa-build \
  --rm \
  ew-zappa-build:${ZAPPA_BUILD_VERSION} bash -c "${SCRIPT}"
