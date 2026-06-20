#!/usr/bin/env bash
set -euo pipefail
export PYTHONPATH="${PYTHONPATH:-}:$(pwd)/src"

python -m vehicle360.drivers.run_pipeline_group \
  --pipeline-group "${1:?SILVER or GOLD required}" \
  --batch-id "${2:-}"
