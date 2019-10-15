#!/bin/sh

# This script automates setting up the DVC jobs for an SQL stage.

set -e
set -x

script="$1"; shift

dvc run -o "$script.transcript" -f "$script.dvc" -d "$script.sql" "$@" python ../run.py sql-script "$script".sql
dvc run -O "$script.status" -d "$script.transcript" --always-changed python ../run.py stage-status "$script"
