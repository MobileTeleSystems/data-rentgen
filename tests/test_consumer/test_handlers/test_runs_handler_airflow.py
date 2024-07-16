import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from faststream.kafka import KafkaBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from uuid6 import UUID

from data_rentgen.db.models import Job, Location, Operation, Run, Status

RESOURCES_PATH = Path(__file__).parent.parent.joinpath("resources").resolve()

pytestmark = [pytest.mark.consumer, pytest.mark.asyncio]


@pytest.fixture
def events_airflow() -> list[dict]:
    lines = (RESOURCES_PATH / "events_airflow.jsonl").read_text().splitlines()
    return [json.loads(line) for line in lines]


@pytest.mark.asyncio
async def test_runs_handler_airflow(
    test_broker: KafkaBroker,
    async_session: AsyncSession,
    events_airflow: list[dict],
):
    for event in events_airflow:
        await test_broker.publish(event, "input.runs")

    # both Spark application & jobs are in the same cluster/host, thus the same location
    job_query = select(Job).order_by(Job.name).options(selectinload(Job.location).selectinload(Location.addresses))
    job_scalars = await async_session.scalars(job_query)
    jobs = job_scalars.all()

    assert len(jobs) == 2
    assert jobs[0].name == "mydag"
    assert jobs[0].location.type == "http"
    assert jobs[0].location.name == "airflow-host:8081"
    assert len(jobs[0].location.addresses) == 1
    assert jobs[0].location.addresses[0].url == "http://airflow-host:8081"

    assert jobs[1].name == "mydag.mytask"
    assert jobs[1].location == jobs[0].location

    run_query = select(Run).order_by(Run.id)
    run_scalars = await async_session.scalars(run_query)
    runs = run_scalars.all()
    assert len(runs) == 2

    dag_run = runs[0]
    assert dag_run.id == UUID("01908223-0782-79b8-9495-b1c38aaee839")
    assert dag_run.created_at == datetime(2024, 7, 5, 9, 4, 12, 162000, tzinfo=timezone.utc)
    assert dag_run.job_id == jobs[0].id
    assert dag_run.status == Status.SUCCEEDED
    assert dag_run.started_at == datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    assert dag_run.ended_at == datetime(2024, 7, 5, 9, 8, 5, 691973, tzinfo=timezone.utc)
    assert dag_run.persistent_log_url is None
    assert dag_run.running_log_url is None
    assert dag_run.started_by_user_id is None

    task_run = runs[1]
    assert task_run.id == UUID("01908223-0782-7fc0-9d69-b1df9dac2c60")
    assert task_run.created_at == datetime(2024, 7, 5, 9, 4, 12, 162000, tzinfo=timezone.utc)
    assert task_run.job_id == jobs[1].id
    assert task_run.parent_run_id == dag_run.id
    assert task_run.status == Status.SUCCEEDED
    assert task_run.started_at == datetime(2024, 7, 5, 9, 4, 20, 783845, tzinfo=timezone.utc)
    assert task_run.ended_at == datetime(2024, 7, 5, 9, 7, 37, 858423, tzinfo=timezone.utc)
    assert task_run.external_id == "manual__2024-07-05T09:04:12.162809+00:00"
    assert task_run.attempt == "1"
    assert task_run.persistent_log_url == (
        "http://airflow-host:8081/dags/mydag/grid?tab=logs&dag_run_id=manual__2024-07-05T09%3A04%3A12.162809%2B00%3A00&task_id=mytask&map_index=-1"
    )
    assert task_run.running_log_url is None
    assert task_run.started_by_user_id is None

    operation_query = select(Operation)
    operation_scalars = await async_session.scalars(operation_query)
    operations = operation_scalars.all()
    assert not operations
