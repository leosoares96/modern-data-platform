#!/usr/bin/env bash
# Wait until a URL answers with a 2xx/3xx status.   wait_for_http.sh <url> [timeout_seconds]
set -euo pipefail

url="${1:?usage: wait_for_http.sh <url> [timeout_seconds]}"
timeout="${2:-120}"
deadline=$((SECONDS + timeout))

until curl -fsS -o /dev/null --max-time 5 "$url"; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "::error::timed out after ${timeout}s waiting for $url" >&2
    exit 1
  fi
  sleep 3
done
echo "up: $url"
