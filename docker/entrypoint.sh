#!/usr/bin/env bash
set -e

python -m arrakis.backend.db.migrations upgrade head
python -m arrakis.backend.db.scripts.create_partitions

exec python -m arrakis.backend --host 0.0.0.0 --port 8000 "$@"
