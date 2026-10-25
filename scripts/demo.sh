#!/usr/bin/env bash
# End-to-end demo. Produces a bounded batch of events, then runs every pipeline stage that exists.
# CI runs this script, so it doubles as the integration test.
#
#   scripts/demo.sh                 # 400 events
#   DEMO_EVENTS=2000 scripts/demo.sh
set -euo pipefail

cd "$(dirname "$0")/.."

EVENTS="${DEMO_EVENTS:-400}"
export PRODUCER_MAX_EVENTS="$EVENTS"
export EVENTS_TICK_SECONDS=0
export PRODUCER_SEED="${PRODUCER_SEED:-42}"

step() { printf '\n==> %s\n' "$*"; }
fail() { printf '::error::%s\n' "$*" >&2; exit 1; }

topic_count() {
  docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:19092 --topic flights.raw --time -1 \
    | awk -F: '{ total += $3 } END { print total + 0 }'
}

step "Starting Kafka and producing $EVENTS events"
docker compose --profile ingest up -d --build
timeout 180 docker wait producer > /dev/null || fail "producer did not finish in time"
[ "$(docker inspect -f '{{.State.ExitCode}}' producer)" = "0" ] || fail "producer exited with an error"

published="$(topic_count)"
echo "messages in flights.raw: $published"
[ "$published" = "$EVENTS" ] || fail "expected $EVENTS messages in flights.raw but found $published"

step "Demo finished"
