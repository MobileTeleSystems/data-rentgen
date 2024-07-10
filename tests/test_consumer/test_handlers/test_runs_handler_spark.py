import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from faststream.kafka import KafkaBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from uuid6 import UUID

from data_rentgen.db.models import Job, Location, Operation, OperationType, Run, Status

RESOURCES_PATH = Path(__file__).parent.parent.joinpath("resources").resolve()

pytestmark = [pytest.mark.consumer, pytest.mark.asyncio]


@pytest.fixture
def events_spark() -> list[dict]:
    lines = (RESOURCES_PATH / "events_spark.jsonl").read_text().splitlines()
    return [json.loads(line) for line in lines]


@pytest.mark.asyncio
async def test_runs_handler_spark(
    test_broker: KafkaBroker,
    async_session: AsyncSession,
    events_spark: list[dict],
):
    for event in events_spark:
        await test_broker.publish(event, "input.runs")

    # both Spark application & jobs are in the same cluster/host, thus the same location
    location_query = select(Location).options(selectinload(Location.addresses))
    location_scalars = await async_session.scalars(location_query)
    locations = location_scalars.all()

    assert len(locations) == 1
    job_location = locations[0]

    assert job_location.type == "host"
    assert job_location.name == "some.host.name"
    assert len(job_location.addresses) == 1
    assert job_location.addresses[0].url == "host://some.host.name"

    job_query = select(Job).order_by(Job.name)
    job_scalars = await async_session.scalars(job_query)
    jobs = job_scalars.all()

    # operations are not handled for now
    assert len(jobs) == 1
    assert jobs[0].name == "spark_session"
    assert jobs[0].location_id == job_location.id

    run_query = select(Run).order_by(Run.id)
    run_scalars = await async_session.scalars(run_query)
    runs = run_scalars.all()
    assert len(runs) == 1

    application_run = runs[0]
    assert application_run.id == UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    assert application_run.created_at == datetime(2024, 7, 5, 9, 5, 49, 584000, tzinfo=timezone.utc)
    assert application_run.job_id == jobs[0].id
    assert application_run.status == Status.SUCCEEDED
    assert application_run.started_at == datetime(2024, 7, 5, 9, 4, 48, 794900, tzinfo=timezone.utc)
    assert application_run.ended_at == datetime(2024, 7, 5, 9, 7, 15, 646000, tzinfo=timezone.utc)
    assert application_run.external_id == "local-1719136537510"
    assert application_run.running_log_url == "http://127.0.0.1:4040"
    assert application_run.persistent_log_url is None

    operation_query = select(Operation).order_by(Operation.id)
    operation_scalars = await async_session.scalars(operation_query)
    operations = operation_scalars.all()
    assert len(operations) == 1

    job_operation = operations[0]
    assert job_operation.id == UUID("01908225-1fd7-746b-910c-70d24f2898b1")
    assert job_operation.created_at == datetime(2024, 7, 5, 9, 6, 29, 463000, tzinfo=timezone.utc)
    assert job_operation.run_id == application_run.id
    assert job_operation.name == "spark_session.execute_save_into_data_source_command"
    assert job_operation.type == OperationType.BATCH
    assert job_operation.status == Status.SUCCEEDED
    assert job_operation.started_at == datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    assert job_operation.ended_at == datetime(2024, 7, 5, 9, 7, 15, 642000, tzinfo=timezone.utc)
    assert job_operation.position == 3
    assert job_operation.description == "Hive -> Clickhouse"
