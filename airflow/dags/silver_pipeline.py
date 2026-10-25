"""Bronze -> silver: deduplicate, apply business rules, run quality checks.

Runs every 15 minutes and publishes the silver dataset so downstream DAGs can react to it.
"""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from dag_common import DEFAULT_ARGS, SILVER_FLIGHTS, START_DATE, spark_submit

with DAG(
    dag_id="silver_pipeline",
    description="Bronze to silver: MERGE INTO, business rules and data quality checks",
    schedule="*/15 * * * *",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["silver", "iceberg", "spark"],
) as dag:
    BashOperator(
        task_id="silver_flights",
        bash_command=spark_submit("batch/silver_job.py"),
        outlets=[SILVER_FLIGHTS],
        execution_timeout=timedelta(minutes=20),
    )
