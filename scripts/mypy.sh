#!/bin/sh
set -e

pip3 install mypy
mypy . --config-file=mypy.ini
