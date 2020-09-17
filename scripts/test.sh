#!/bin/sh
set -e

./scripts/mypy.sh
pytest --spec -vvvv
