#!/bin/sh
set -e

mypy .
python3 server.py
