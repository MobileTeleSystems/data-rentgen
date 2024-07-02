#!/usr/bin/env bash
set -e

exec python -m arrakis.consumer "$@"
