#!/usr/bin/env bash
set -e

exec python -m data_rentgen.consumer "$@"
