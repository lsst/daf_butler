#!/bin/sh
pip install uv

REQUIREMENTS_DIR=$(dirname "$0")
uv pip compile --upgrade --generate-hashes --build-isolation --output-file $REQUIREMENTS_DIR/docker.txt $REQUIREMENTS_DIR/docker.in
uv pip compile --upgrade --generate-hashes --build-isolation --output-file $REQUIREMENTS_DIR/docker-test.txt $REQUIREMENTS_DIR/docker-test.in
