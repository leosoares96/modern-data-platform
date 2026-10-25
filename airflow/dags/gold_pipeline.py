"""Silver -> gold aggregates.

Not scheduled by time: it runs whenever the silver DAG has published new data.
"""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from dag_common import DEFAULT_ARGS, GOLD_FLIGHTS, SILVER_FLIGHTS, START_DATE, spark_submit

with DAG(
    dag_id="gold_pipeline",
    description="Silver to gold: hourly aggregates and the activity grid",
    schedule=[SILVER_FLIGHTS],
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["gold", "iceberg", "spark"],
) as dag:
    BashOperator(
        task_id="gold_flights",
        bash_command=spark_submit("batch/gold_job.py"),
        outlets=[GOLD_FLIGHTS],
        execution_timeout=timedelta(minutes=20),
    )
