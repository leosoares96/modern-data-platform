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
# Dirty data on purpose, so every defensive path in the pipeline is exercised.
export DUPLICATE_RATE="${DUPLICATE_RATE:-0.05}"
export LATE_RATE="${LATE_RATE:-0.10}"
export MALFORMED_RATE="${MALFORMED_RATE:-0.05}"

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

stage_bronze() {
  step "Bronze: draining Kafka into Iceberg with Spark Structured Streaming"
  submit /opt/platform/spark/streaming/adsb_stream.py --once
  submit /opt/platform/spark/admin/verify.py bronze --events "$EVENTS"
}

stage_silver() {
  step "Silver: deduplicate with MERGE INTO, apply business rules, run quality checks"
  submit /opt/platform/spark/batch/silver_job.py
  submit /opt/platform/spark/admin/verify.py silver --events "$EVENTS"

  step "Silver is idempotent: running it again must not insert anything"
  local second
  second="$(submit /opt/platform/spark/batch/silver_job.py | tee /dev/stderr)"
  echo "$second" | grep -Eq "inserted=0 |nothing new in bronze"     || fail "a second silver run inserted rows"
}

stage_gold() {
  step "Gold: hourly aggregates and an activity grid, recomputed only for changed hours"
  submit /opt/platform/spark/batch/gold_job.py
  submit /opt/platform/spark/batch/gold_job.py   # again: must not double count
  submit /opt/platform/spark/admin/verify.py gold --events "$EVENTS"
}

stage_iceberg() {
  step "Iceberg showcase: time travel, rollback, schema and partition evolution"
  submit /opt/platform/spark/demo/time_travel.py
}

stage_maintenance() {
  step "Maintenance: compaction and snapshot expiry must not change the data"
  local before after
  before="$(submit /opt/platform/spark/admin/verify.py counts | grep '^count ' || true)"
  submit /opt/platform/spark/batch/maintenance_job.py
  after="$(submit /opt/platform/spark/admin/verify.py counts | grep '^count ' || true)"
  echo "$before"
  [ -n "$before" ] || fail "could not read the table counts"
  [ "$before" = "$after" ] || fail "maintenance changed the data: before=[$before] after=[$after]"
}

stage_orchestration() {
  step "Orchestration: Airflow runs the same jobs, and re-running them changes nothing"
  # Start with every DAG paused so the scheduler cannot run a job while we test it by hand: two
  # concurrent MERGE commits on the same Iceberg table conflict.
  export AIRFLOW_DAGS_PAUSED=true
  "${COMPOSE[@]}" up -d --build postgres airflow
  scripts/wait_for_http.sh http://localhost:8088/health 300

  local airflow=("${COMPOSE[@]}" exec -T airflow)
  local dags
  dags="$("${airflow[@]}" airflow dags list -o plain)"
  for dag in silver_pipeline gold_pipeline maintenance_pipeline; do
    echo "$dags" | grep -q "$dag" || fail "Airflow did not load the DAG $dag"
  done

  local before after
  before="$(submit /opt/platform/spark/admin/verify.py counts | grep '^count ' || true)"
  [ -n "$before" ] || fail "could not read the table counts"
  "${airflow[@]}" airflow tasks test silver_pipeline silver_flights 2026-01-01
  "${airflow[@]}" airflow tasks test gold_pipeline gold_flights 2026-01-01
  "${airflow[@]}" airflow tasks test maintenance_pipeline maintain_tables 2026-01-01
  after="$(submit /opt/platform/spark/admin/verify.py counts | grep '^count ' || true)"
  [ "$before" = "$after" ] || fail "the DAG tasks changed the data: before=[$before] after=[$after]"

  step "Orchestration: the scheduler runs silver, and silver's dataset triggers gold"
  "${airflow[@]}" airflow dags unpause silver_pipeline
  "${airflow[@]}" airflow dags unpause gold_pipeline
  "${airflow[@]}" airflow dags trigger silver_pipeline

  local runs deadline=$((SECONDS + 420))
  while true; do
    runs="$("${airflow[@]}" airflow dags list-runs -d gold_pipeline -o plain)"
    echo "$runs" | grep -Eq "dataset_triggered.*failed" && fail "the dataset-triggered gold run failed"
    echo "$runs" | grep -Eq "dataset_triggered.*success" && break
    [ "$SECONDS" -lt "$deadline" ] || fail "gold was not triggered by the silver dataset in time"
    sleep 15
  done
  echo "gold ran because silver published its dataset"
}

if [ "${DEMO_CLEAN:-1}" = "1" ]; then
  step "Starting from a clean state (set DEMO_CLEAN=0 to keep existing data)"
  "${COMPOSE[@]}" down -v --remove-orphans
fi

stage_ingest
stage_lake
stage_bronze
stage_silver
stage_gold
stage_iceberg
stage_maintenance
stage_orchestration

step "Demo finished"
