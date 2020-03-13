#!/bin/sh
set -e

./scripts/test.sh
mypy .

pip3 install .
