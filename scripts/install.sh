#!/bin/sh
set -e

./scripts/test
mypy .
pip3 install .
