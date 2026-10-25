#!/usr/bin/env bash
# Stop every service and delete all volumes (Kafka offsets, object store, Airflow database).
# This destroys the lakehouse data. Pass --yes to skip the confirmation.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ "${1:-}" != "--yes" ]; then
  read -r -p "This deletes all platform data. Continue? [y/N] " answer
  case "$answer" in
    y | Y) ;;
    *) echo "Aborted."; exit 1 ;;
  esac
fi

docker compose --profile all down -v --remove-orphans
echo "Environment reset."
