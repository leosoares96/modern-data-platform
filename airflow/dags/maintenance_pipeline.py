"""Weekly Iceberg table maintenance: compaction, snapshot expiry and orphan file removal."""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from dag_common import DEFAULT_ARGS, START_DATE, spark_submit

with DAG(
    dag_id="maintenance_pipeline",
    description="Compact small files and expire old snapshots on every table",
    schedule="@weekly",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["maintenance", "iceberg", "spark"],
) as dag:
    BashOperator(
        task_id="maintain_tables",
        bash_command=spark_submit(
            "batch/maintenance_job.py", "--retain-last 5 --older-than-hours 168"
        ),
        execution_timeout=timedelta(hours=1),
    )
