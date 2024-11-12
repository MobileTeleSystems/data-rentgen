from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
from random import choice, randint
from typing import AsyncContextManager, Callable

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Address, Job, Run, RunStartReason, RunStatus, User
from data_rentgen.db.utils.uuid import extract_timestamp_from_uuid, generate_new_uuid
from tests.test_server.fixtures.factories.base import random_datetime, random_string
from tests.test_server.fixtures.factories.job import job_factory
from tests.test_server.utils.delete import clean_db


def run_factory(**kwargs):
    created_at = kwargs.pop("created_at", None)
    run_id = generate_new_uuid(created_at)
    data = {
        "created_at": extract_timestamp_from_uuid(run_id),
        "id": run_id,
        "job_id": randint(0, 10000000),
        "parent_run_id": generate_new_uuid(),
        "status": choice(list(RunStatus)),
        "external_id": random_string(128),
        "attempt": random_string(16),
        "persistent_log_url": random_string(32),
        "running_log_url": random_string(32),
        "started_at": random_datetime(),
        "started_by_user_id": randint(0, 10000000),
        "start_reason": choice(list(RunStartReason)),
        "ended_at": random_datetime(),
        "end_reason": random_string(8),
    }
    data.update(kwargs)
    return Run(**data)


async def create_run(
    async_session: AsyncSession,
    run_kwargs: dict | None = None,
) -> Run:
    run_kwargs = run_kwargs or {}
    run = run_factory(**run_kwargs)
    async_session.add(run)
    await async_session.commit()
    await async_session.refresh(run)

    return run


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
    job: Job,
    user: User,
) -> AsyncGenerator[Run, None]:
    params = request.param
    params.update({"started_by_user_id": user.id})
    params.update({"job_id": job.id})
    async with async_session_maker() as async_session:
        run = await create_run(async_session, run_kwargs=params)

    yield run

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture(params=[(10, {})])
async def runs(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    user: User,
    jobs: list[Job],
) -> AsyncGenerator[list[Run], None]:
    size, params = request.param
    started_at = datetime.now()

    async with async_session_maker() as async_session:
        items = [
            await create_run(
                async_session,
                run_kwargs={
                    "job_id": jobs[i // 2].id,
                    "created_at": started_at + timedelta(seconds=0.1 * i),
                    "started_by_user_id": user.id,
                    **params,
                },
            )
            for i in range(size)
        ]

        async_session.expunge_all()

    yield items

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture(params=[(5, {})])
async def runs_with_same_job(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    user: User,
    job: Job,
) -> AsyncGenerator[list[Run], None]:
    size, params = request.param
    started_at = datetime.now()

    async with async_session_maker() as async_session:
        items = [
            await create_run(
                async_session,
                run_kwargs={
                    "job_id": job.id,
                    "created_at": started_at + timedelta(seconds=s),  # To be sure runs has different timestamps
                    "started_by_user_id": user.id,
                    **params,
                },
            )
            for s in range(size)
        ]

        async_session.expunge_all()

    yield items

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture(params=[(5, {})])
async def runs_with_same_parent(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    user: User,
) -> AsyncGenerator[list[Run], None]:
    size, params = request.param
    started_at = datetime.now()
    parent_run_id = generate_new_uuid()

    async with async_session_maker() as async_session:
        items = [
            await create_run(
                async_session,
                run_kwargs={
                    "parent_run_id": parent_run_id,
                    "created_at": started_at + timedelta(seconds=s),  # To be sure runs has different timestamps
                    "started_by_user_id": user.id,
                    **params,
                },
            )
            for s in range(size)
        ]

        async_session.expunge_all()

    yield items

    async with async_session_maker() as async_session:
        await clean_db(async_session)


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
