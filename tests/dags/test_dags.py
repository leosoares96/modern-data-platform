from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402

DAG_FOLDER = Path(__file__).resolve().parents[2] / "airflow" / "dags"
EXPECTED = {"silver_pipeline", "gold_pipeline", "maintenance_pipeline"}


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAG_FOLDER), include_examples=False)


def test_every_dag_imports_without_errors(dagbag):
    assert dagbag.import_errors == {}


def test_the_expected_dags_exist(dagbag):
    assert set(dagbag.dag_ids) == EXPECTED


def test_every_dag_has_tags_an_owner_and_no_catchup(dagbag):
    for dag in dagbag.dags.values():
        assert dag.tags, f"{dag.dag_id} has no tags"
        assert dag.default_args["owner"] == "data-platform"
        assert dag.catchup is False


def test_runs_never_overlap(dagbag):
    for dag in dagbag.dags.values():
        assert dag.max_active_runs == 1, f"{dag.dag_id} could run concurrently with itself"


def test_every_task_retries_and_has_a_timeout(dagbag):
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            assert task.retries >= 1, f"{dag.dag_id}.{task.task_id} never retries"
            assert task.execution_timeout is not None, f"{dag.dag_id}.{task.task_id} can hang"


def test_gold_runs_when_silver_publishes_its_dataset(dagbag):
    silver_tasks = dagbag.dags["silver_pipeline"].tasks
    published = {outlet.uri for task in silver_tasks for outlet in task.outlets}
    condition = dagbag.dags["gold_pipeline"].timetable.dataset_condition
    waits_for = {uri for uri, _ in condition.iter_datasets()}
    assert published
    assert published == waits_for


def test_tasks_submit_spark_jobs_that_exist(dagbag):
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            command = task.bash_command
            assert "submit.sh" in command
            job = command.split("/opt/platform/spark/")[1].split()[0]
            assert (DAG_FOLDER.parents[1] / "spark" / job).is_file(), f"{job} does not exist"
