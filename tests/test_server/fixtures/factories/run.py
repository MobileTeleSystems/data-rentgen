import time
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
from random import choice, randint
from typing import AsyncContextManager, Callable

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Job, Run, Status, User
from data_rentgen.db.utils.uuid import extract_timestamp_from_uuid, generate_new_uuid
from tests.test_server.fixtures.factories.base import random_datetime, random_string


def run_factory_minimal(**kwargs):
    if kwargs.get("instant"):
        run_id = generate_new_uuid(kwargs.pop("instant"))
    else:
        time.sleep(0.1)  # To be sure runs has different timestamps
        run_id = generate_new_uuid()
    data = {
        "created_at": extract_timestamp_from_uuid(run_id),
        "id": run_id,
        "job_id": randint(0, 10000000),
    }
    data.update(kwargs)
    return Run(**data)


def run_factory(**kwargs):
    time.sleep(0.1)
    run_id = generate_new_uuid()
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
    job: Job,
) -> AsyncGenerator[Run, None]:
    params = request.param
    item = run_factory_minimal(job_id=job.id, **params)

    # TODO: Refactor this part to separate function.
    async with async_session_maker() as async_session:
        async_session.add(item)
        # this is not required for backend tests, but needed by client tests
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        await async_session.refresh(item)
        async_session.expunge(item)

    yield item


@pytest_asyncio.fixture(params=[(5, {})])
async def runs(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    jobs: list[Job],
) -> AsyncGenerator[list[Run], None]:
    size, params = request.param
    items = [run_factory_minimal(job_id=choice(jobs).id, **params) for _ in range(size)]

    # TODO: Refactor this part to separate function.
    async with async_session_maker() as async_session:
        for item in items:
            async_session.add(item)
        # this is not required for backend tests, but needed by client tests
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        for item in items:
            await async_session.refresh(item)
            async_session.expunge(item)

    yield items


@pytest_asyncio.fixture(params=[(5, {})])
async def runs_with_one_job(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    job: Job,
) -> AsyncGenerator[list[Run], None]:
    size, params = request.param
    started_at = datetime.now()
    items = [
        run_factory_minimal(
            job_id=job.id,
            instant=started_at + timedelta(seconds=s),  # To be sure runs has different timestamps
            **params,
        )
        for s in range(size)
    ]

    async with async_session_maker() as async_session:
        for item in items:
            async_session.add(item)
        # this is not required for backend tests, but needed by client tests
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        for item in items:
            await async_session.refresh(item)
            async_session.expunge(item)

    yield items


@pytest_asyncio.fixture(params=[{}])
async def full_run(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    jobs: list[Job],
    user: User,
) -> AsyncGenerator[Run, None]:
    params = request.param
    parent_run = run_factory_minimal(job_id=jobs[0].id, **params)
    item = run_factory(job_id=jobs[1].id, started_by_user_id=user.id, parent_run_id=parent_run.id, **params)

    async with async_session_maker() as async_session:
        async_session.add(parent_run)
        async_session.add(item)
        # this is not required for backend tests, but needed by client tests
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        await async_session.refresh(item)
        async_session.expunge(item)

    yield item
