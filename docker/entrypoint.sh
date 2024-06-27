#!/usr/bin/env bash
set -e

python -m arrakis.backend.db.migrations upgrade head

exec python -m arrakis.backend --host 0.0.0.0 --port 8000 "$@"
