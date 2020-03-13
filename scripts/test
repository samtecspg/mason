#!/bin/sh
set -e

mypy .
pytest --spec -vvvv --cov=. --cov-report html
