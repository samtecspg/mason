#!/bin/sh
set -e

pytest --spec -vvvv --cov=. --cov-report html
