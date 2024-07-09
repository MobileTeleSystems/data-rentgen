from collections.abc import AsyncGenerator
from random import randint
from typing import AsyncContextManager, Callable

import pytest
import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models.job import Job, Location
from tests.test_server.fixtures.factories.base import random_string


def job_factory(**kwargs):
    data = {
        "id": randint(0, 10000000),
        "location_id": randint(0, 10000000),
        "name": random_string(32),
    }
    data.update(kwargs)
    return Job(**data)


@pytest_asyncio.fixture(params=[{}])
async def new_job(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
) -> AsyncGenerator[Job, None]:
    params = request.param
    item = job_factory(**params)

    yield item

    query = delete(Job).where(Job.id == item.id)
    async with async_session_maker() as session:
        await session.execute(query)
        await session.commit()


@pytest_asyncio.fixture(params=[{}])
async def job(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    location: Location,
) -> AsyncGenerator[Job, None]:
    params = request.param
    item = job_factory(location_id=location.id, **params)
    del item.id

    async with async_session_maker() as async_session:
        async_session.add(item)
        # this is not required for backend tests, but needed by client tests
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        job_id = item.id
        await async_session.refresh(item)
        async_session.expunge(item)

    yield item

    query = delete(Job).where(Job.id == job_id)
    async with async_session_maker() as async_session:
        await async_session.execute(query)
        await async_session.commit()


@pytest_asyncio.fixture(params=[(5, {})])
async def jobs(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    location: Location,
) -> AsyncGenerator[list[Job], None]:
    size, params = request.param
    items = [job_factory(location_id=location.id, **params) for _ in range(size)]

    async with async_session_maker() as async_session:
        for item in items:
            del item.id
            async_session.add(item)
        # this is not required for backend tests, but needed by client tests
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        job_ids = []
        for item in items:
            job_ids.append(item.id)
            await async_session.refresh(item)
            async_session.expunge(item)

    yield items

    query = delete(Job).where(Job.id.in_(job_ids))
    async with async_session_maker() as async_session:
        await async_session.execute(query)
        await async_session.commit()
