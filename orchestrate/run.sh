#!/bin/bash
set -e

cd /Users/waldchiller/Projects/duck_dwh

source /Users/waldchiller/Projects/duck_dwh/.venv/bin/activate

echo "==> Running ingest..."
python ingest/ingest.py

#add error and retry logic at some point

echo "==> Running dbt build..."
cd /Users/waldchiller/Projects/duck_dwh/bookworm
dbt build --select staging

echo "==> Done."
