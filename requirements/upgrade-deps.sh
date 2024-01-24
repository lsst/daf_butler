#!/bin/sh
pip install pip-tools

REQUIREMENTS_DIR=$(dirname "$0")
pip-compile --upgrade --generate-hashes --build-isolation --output-file $REQUIREMENTS_DIR/docker.txt $REQUIREMENTS_DIR/docker.in
pip-compile --upgrade --generate-hashes --build-isolation --output-file $REQUIREMENTS_DIR/docker-test.txt $REQUIREMENTS_DIR/docker-test.in
