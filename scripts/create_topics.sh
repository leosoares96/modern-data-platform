#!/bin/sh
# Create the Kafka topics the platform needs. Idempotent: safe to run on every start.
# Runs inside the kafka-init container, but works anywhere kafka-topics.sh is on the path.
set -eu

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:19092}"
KAFKA_TOPICS="${KAFKA_TOPICS_BIN:-/opt/kafka/bin/kafka-topics.sh}"

create_topic() {
  name="$1"; partitions="$2"; retention_ms="$3"
  "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor 1 \
    --config "retention.ms=$retention_ms"
  echo "topic ready: $name (partitions=$partitions retention_ms=$retention_ms)"
}

# 7 days of retention is plenty: the bronze job reads continuously and Iceberg is the system of record.
create_topic "${KAFKA_TOPIC:-flights.raw}" 3 604800000
