#!/usr/bin/env bash
# Start the stack for one or more profiles and wait until every service with a healthcheck is healthy.
#
#   scripts/bootstrap.sh                   # all profiles
#   scripts/bootstrap.sh ingest            # Kafka and the producer only
#   scripts/bootstrap.sh lake stream       # several profiles
set -euo pipefail

cd "$(dirname "$0")/.."

profiles=("$@")
[ "${#profiles[@]}" -eq 0 ] && profiles=(all)

flags=()
for p in "${profiles[@]}"; do flags+=(--profile "$p"); done

docker compose "${flags[@]}" up -d --build

echo "Waiting for healthchecks (timeout ${BOOTSTRAP_TIMEOUT:-300}s)..."
deadline=$((SECONDS + ${BOOTSTRAP_TIMEOUT:-300}))
while true; do
  pending=()
  for id in $(docker compose "${flags[@]}" ps -q); do
    status=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$id")
    if [ "$status" != "healthy" ] && [ "$status" != "none" ]; then
      pending+=("$(docker inspect -f '{{.Name}}' "$id" | sed 's#^/##'):$status")
    fi
  done
  [ "${#pending[@]}" -eq 0 ] && break
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "Timed out waiting for: ${pending[*]}" >&2
    exit 1
  fi
  sleep 5
done
echo "Ready."
docker compose "${flags[@]}" ps
