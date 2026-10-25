"""Shared definitions for the platform DAGs."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.datasets import Dataset

SPARK_JOBS = "/opt/platform/spark"
SUBMIT = "bash /opt/platform/scripts/submit.sh"

# Datasets let a DAG run when another one has produced data, instead of guessing with schedules.
SILVER_FLIGHTS = Dataset("iceberg://lake/silver.flights")
GOLD_FLIGHTS = Dataset("iceberg://lake/gold.flights_hourly")

START_DATE = pendulum.datetime(2026, 1, 1, tz="UTC")

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def spark_submit(job: str, arguments: str = "") -> str:
    """Shell command that runs a PySpark job on the cluster through scripts/submit.sh."""
    return f"{SUBMIT} {SPARK_JOBS}/{job} {arguments}".strip()
