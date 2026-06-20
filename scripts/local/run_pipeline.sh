#!/usr/bin/env bash
set -euo pipefail
export PYTHONPATH="${PYTHONPATH:-}:$(pwd)/src"

python -m vehicle360.drivers.run_pipeline \
  --pipeline-id "${1:?pipeline id required}" \
  --batch-id "${2:?batch id required}" \
  --layer "${3:?bronze_to_silver or silver_to_gold required}"
