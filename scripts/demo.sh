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

COMPOSE=(docker compose --profile all)

step() { printf '\n==> %s\n' "$*"; }
fail() { printf '::error::%s\n' "$*" >&2; exit 1; }

# Run a PySpark job on the cluster from the long-lived spark-client container.
submit() { "${COMPOSE[@]}" exec -T spark-client bash /opt/platform/scripts/submit.sh "$@"; }

topic_count() {
  docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:19092 --topic flights.raw --time -1 \
    | awk -F: '{ total += $3 } END { print total + 0 }'
}

stage_ingest() {
  step "Starting Kafka and producing $EVENTS events"
  "${COMPOSE[@]}" up -d --build kafka kafka-init producer
  timeout 180 docker wait producer > /dev/null || fail "producer did not finish in time"
  [ "$(docker inspect -f '{{.State.ExitCode}}' producer)" = "0" ] || fail "producer exited with an error"

  local published
  published="$(topic_count)"
  echo "messages in flights.raw: $published"
  [ "$published" = "$EVENTS" ] || fail "expected $EVENTS messages in flights.raw but found $published"
}

stage_lake() {
  step "Starting the object store, the Iceberg catalog and the Spark cluster"
  "${COMPOSE[@]}" up -d --build minio minio-init iceberg-rest spark-master spark-worker spark-client
  scripts/wait_for_http.sh http://localhost:8181/v1/config 180
  scripts/wait_for_http.sh http://localhost:8080 120

  step "Creating the lakehouse tables"
  submit /opt/platform/spark/admin/init_lakehouse.py

  step "Smoke test: write and read an Iceberg table"
  submit /opt/platform/spark/admin/smoke_test.py
}

stage_ingest
stage_lake

step "Demo finished"
