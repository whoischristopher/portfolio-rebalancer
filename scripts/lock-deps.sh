#!/bin/sh
# Regenerate requirements.txt (the lock file) from requirements.in, with hashes.
#
# Run it in the same Python as the Docker image so the resolution matches, e.g.
#   docker run --rm -v "$PWD":/w -w /w -u "$(id -u):$(id -g)" -e HOME=/tmp python:3.13-slim sh scripts/lock-deps.sh
#
# Add --upgrade (or --upgrade-package NAME) to move pins forward within requirements.in's ranges:
#   ... sh scripts/lock-deps.sh --upgrade
# CI runs this too and fails if the committed requirements.txt doesn't match.
set -eu
PIP_TOOLS_VERSION=7.6.2

python -m pip install --quiet --disable-pip-version-check --user "pip-tools==${PIP_TOOLS_VERSION}"
python -m piptools compile --quiet --generate-hashes --strip-extras --no-emit-index-url \
    --output-file=requirements.txt "$@" requirements.in
echo "requirements.txt updated."
