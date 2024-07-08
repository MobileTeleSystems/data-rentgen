import json
from pathlib import Path

import pytest
from faststream.kafka import KafkaBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from data_rentgen.db.models.job import Job
from data_rentgen.db.models.location import Location

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
