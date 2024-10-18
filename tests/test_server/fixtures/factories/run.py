from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
from random import choice, randint
from typing import AsyncContextManager, Callable

import pytest
import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Address, Job, Run, Status, User
from data_rentgen.db.utils.uuid import extract_timestamp_from_uuid, generate_new_uuid
from tests.test_server.fixtures.factories.base import random_datetime, random_string
from tests.test_server.fixtures.factories.job import job_factory


def run_factory(**kwargs):
    created_at = kwargs.pop("created_at", None)
    run_id = generate_new_uuid(created_at)
    data = {
        "created_at": extract_timestamp_from_uuid(run_id),
        "id": run_id,
        "job_id": randint(0, 10000000),
        "parent_run_id": generate_new_uuid(),
        "status": choice(list(Status)),
        "external_id": random_string(128),
        "attempt": random_string(16),
        "persistent_log_url": random_string(32),
        "running_log_url": random_string(32),
        "started_at": random_datetime(),
        "started_by_user_id": randint(0, 10000000),
        "start_reason": choice(["MANUAL", "AUTOMATIC"]),
        "ended_at": random_datetime(),
        "end_reason": random_string(8),
    }
    data.update(kwargs)
    return Run(**data)


@pytest_asyncio.fixture(params=[{}])
async def new_run(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[Run, None]:
    params = request.param
    item = run_factory(**params)

    yield item


@pytest_asyncio.fixture(params=[{}])
async def run(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    user: User,
    job: Job,
) -> AsyncGenerator[Run, None]:
    params = request.param
    item = run_factory(job_id=job.id, started_by_user_id=user.id, **params)

    # TODO: Refactor this part to separate function.
    async with async_session_maker() as async_session:
        async_session.add(item)

        await async_session.commit()
        await async_session.refresh(item)

        async_session.expunge_all()

    yield item

    delete_query = delete(Run).where(Run.id == item.id)
    # Add teardown cause fixture async_session doesn't used
    async with async_session_maker() as async_session:
        await async_session.execute(delete_query)
        await async_session.commit()


@pytest_asyncio.fixture(params=[(10, {})])
async def runs(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    user: User,
    jobs: list[Job],
) -> AsyncGenerator[list[Run], None]:
    size, params = request.param
    started_at = datetime.now()
    items = [
        run_factory(
            job_id=jobs[i // 2].id,
            created_at=started_at + timedelta(seconds=0.1 * i),
            started_by_user_id=user.id,
            **params,
        )
        for i in range(size)
    ]

    # TODO: Refactor this part to separate function.
    async with async_session_maker() as async_session:
        async_session.add_all(items)

        await async_session.commit()
        for item in items:
            await async_session.refresh(item)

        async_session.expunge_all()

    yield items

    delete_query = delete(Run).where(Run.id.in_([item.id for item in items]))
    # Add teardown cause fixture async_session doesn't used
    async with async_session_maker() as async_session:
        await async_session.execute(delete_query)
        await async_session.commit()


@pytest_asyncio.fixture(params=[(5, {})])
async def runs_with_same_job(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    user: User,
    job: Job,
) -> AsyncGenerator[list[Run], None]:
    size, params = request.param
    started_at = datetime.now()
    items = [
        run_factory(
            job_id=job.id,
            created_at=started_at + timedelta(seconds=s),  # To be sure runs has different timestamps
            started_by_user_id=user.id,
            **params,
        )
        for s in range(size)
    ]

    async with async_session_maker() as async_session:
        async_session.add_all(items)

        await async_session.commit()
        for item in items:
            await async_session.refresh(item)

        async_session.expunge_all()

    yield items

    delete_query = delete(Run).where(Run.id.in_([item.id for item in items]))
    # Add teardown cause fixture async_session doesn't used
    async with async_session_maker() as async_session:
        await async_session.execute(delete_query)
        await async_session.commit()


@pytest_asyncio.fixture(params=[(5, {})])
async def runs_with_same_parent(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    user: User,
    jobs: list[Job],
) -> AsyncGenerator[list[Run], None]:
    size, params = request.param
    started_at = datetime.now()
    parrent_run_id = generate_new_uuid()
    items = [
        run_factory(
            parent_run_id=parrent_run_id,
            created_at=started_at + timedelta(seconds=s),  # To be sure runs has different timestamps
            started_by_user_id=user.id,
            **params,
        )
        for s in range(size)
    ]

    async with async_session_maker() as async_session:
        async_session.add_all(items)

        await async_session.commit()
        for item in items:
            await async_session.refresh(item)

        async_session.expunge_all()

    yield items

    delete_query = delete(Run).where(Run.id.in_([item.id for item in items]))
    # Add teardown cause fixture async_session doesn't used
    async with async_session_maker() as async_session:
        await async_session.execute(delete_query)
        await async_session.commit()


@pytest_asyncio.fixture(params=[{}])
async def runs_search(
    request: pytest.FixtureRequest,
    async_session: AsyncSession,
    addresses: list[Address],
    user: User,
) -> AsyncGenerator[dict[str | None, Run], None]:
    request.param
    job_names_type = [("spark_application_name", "SPARK_APPLICATION"), ("airflow_dag_name", "AIRFLOW_DAG")]
    jobs = [
        job_factory(
            name=name,
            type=job_type,
            location_id=choice(addresses).location_id,
        )
        for name, job_type in job_names_type
    ]
    for item in jobs:
        del item.id
        async_session.add(item)
    await async_session.flush()

    runs_external_ids = [
        "application_1638922609021_0001",
        "application_1638922609021_0002",
        "extract_task_0001",
        "extract_task_0002",
    ]

    started_at = datetime.now()
    runs = [
        run_factory(
            created_at=started_at + timedelta(seconds=0.1 * i),
            external_id=external_id,
            job_id=job.id,
            started_by_user_id=user.id,
        )
        # Each job has 2 runs
        for i, (external_id, job) in enumerate(zip(runs_external_ids, [job for job in jobs for _ in range(2)]))
    ]
    for item in runs:
        async_session.add(item)
    await async_session.commit()

    for item in jobs + runs:
        await async_session.refresh(item)
        async_session.expunge(item)

    yield {run.external_id: run for run in runs}
