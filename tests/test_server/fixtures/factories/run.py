from __future__ import annotations

from datetime import UTC, datetime, timedelta
from random import choice, randint
from typing import TYPE_CHECKING

import pytest_asyncio

from data_rentgen.db.models import Job, Run, RunStartReason, RunStatus, User
from data_rentgen.utils.uuid import extract_timestamp_from_uuid, generate_new_uuid
from tests.test_server.fixtures.factories.base import random_datetime, random_string
from tests.test_server.fixtures.factories.job import create_job
from tests.test_server.fixtures.factories.job_type import create_job_type
from tests.test_server.fixtures.factories.location import create_location
from tests.test_server.utils.delete import clean_db

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable
    from contextlib import AbstractAsyncContextManager

    import pytest
    from sqlalchemy.ext.asyncio import AsyncSession


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
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
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
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
    jobs: list[Job],
) -> AsyncGenerator[list[Run], None]:
    size, params = request.param
    started_at = datetime.now(tz=UTC)

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
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
    job: Job,
) -> AsyncGenerator[list[Run], None]:
    size, params = request.param
    started_at = datetime.now(tz=UTC)

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
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
) -> AsyncGenerator[list[Run], None]:
    size, params = request.param
    started_at = datetime.now(tz=UTC)
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


@pytest_asyncio.fixture()
async def runs_search(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
) -> AsyncGenerator[dict[str | None, Run], None]:
    job_kwargs = [
        {"name": "spark_application_name", "type": "SPARK_APPLICATION"},
        {"name": "airflow_dag_name", "type": "AIRFLOW_DAG"},
    ]
    runs_kwargs = [
        {"external_id": "application_1638922609021_0001"},
        {"external_id": "application_1638922609021_0002"},
        {"external_id": "extract_task_0001"},
        {"external_id": "extract_task_0002"},
    ]
    started_at = datetime.now(tz=UTC)
    async with async_session_maker() as async_session:
        jobs = []
        for kwargs in job_kwargs:
            location = await create_location(async_session)
            job_type = await create_job_type(async_session, job_type_kwargs={"type": kwargs["type"]})
            jobs.append(
                await create_job(
                    async_session,
                    location_id=location.id,
                    job_type_id=job_type.id,
                    job_kwargs=kwargs,
                ),
            )
        runs = [
            await create_run(
                async_session,
                run_kwargs={
                    "created_at": started_at + timedelta(seconds=0.1 * i),
                    "job_id": job.id,
                    "started_by_user_id": user.id,
                    **kwargs,
                },
            )
            for i, (job, kwargs) in enumerate(zip([job for job in jobs for _ in range(2)], runs_kwargs, strict=False))
        ]

        async_session.expunge_all()

    yield {run.external_id: run for run in runs}

    async with async_session_maker() as async_session:
        await clean_db(async_session)
