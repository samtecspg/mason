#!/bin/sh
set -e

pip3 install -r requirements_test.txt
pip3 install -r requirements.txt

./scripts/mypy.sh
pytest --spec -vvvv
