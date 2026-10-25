#!/usr/bin/env bash
# Run a PySpark job on the standalone cluster in client mode.
#
#   submit.sh /opt/platform/spark/batch/silver_job.py [job arguments...]
#
# Used inside containers by Compose, by `make` targets and by the Airflow DAGs, so every job runs
# with the same resource limits. Override them with the SPARK_* variables below.
set -euo pipefail

JOB="${1:?usage: submit.sh <job.py> [args...]}"
shift

export PYTHONPATH="/opt/platform/spark:${PYTHONPATH:-}"

# Executors connect back to the driver, so advertise this container's IP instead of its hostname.
DRIVER_HOST="${SPARK_DRIVER_HOST:-$(getent hosts "$(hostname)" | awk '{print $1; exit}')}"

exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --deploy-mode client \
  --conf "spark.driver.host=${DRIVER_HOST}" \
  --conf "spark.driver.bindAddress=0.0.0.0" \
  --conf "spark.cores.max=${SPARK_CORES_MAX:-2}" \
  --conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY:-1g}" \
  --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY:-1g}" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS:-8}" \
  "$JOB" "$@"
