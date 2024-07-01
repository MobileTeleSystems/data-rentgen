#!/usr/bin/env bash
set -e

python -m arrakis.db.migrations upgrade head
python -m arrakis.db.scripts.create_partitions

exec python -m arrakis.server --host 0.0.0.0 --port 8000 "$@"
