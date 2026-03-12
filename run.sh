#!/bin/bash
set -e

cd /Users/waldchiller/Projects/duck_dwh

source .venv/bin/activate

echo "==> Running ingest..."
python ingest.py

echo "==> Running dbt build..."
cd bookworm
dbt build

echo "==> Done."
