#!/bin/sh
set -e

pip3 install -r requirements_test.txt

./scripts/mypy.sh
pytest --spec -vvvv
