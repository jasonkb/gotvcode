#!/usr/bin/env bash

# Install the zappa-build image in a way that can be referenced consistently by
# other scripts.
#
# Bump the version if the image changes. Scripts that require the change can reference
# the new version number and will fail if it's not installed on the developer's machine
VERSION=2
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd)"

docker build -t ew-zappa-build:${VERSION} "${DIR}"
