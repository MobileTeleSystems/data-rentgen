#!/usr/bin/env bash
set -e

python -m data_rentgen.db.migrations upgrade head
python -m data_rentgen.db.scripts.create_partitions

exec python -m data_rentgen.server --host 0.0.0.0 --port 8000 "$@"
