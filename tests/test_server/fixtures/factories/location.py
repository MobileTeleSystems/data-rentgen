from collections.abc import AsyncGenerator
from random import choice, randint
from typing import AsyncContextManager, Callable

import pytest
import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models.job import Location
from tests.test_server.fixtures.factories.base import random_string


def location_factory(**kwargs):
    data = {
        "id": randint(0, 10000000),
        "type": choice(["kafka", "postgres", "hdfs"]),
        "name": random_string(),
    }
    data.update(kwargs)
    return Location(**data)


@pytest_asyncio.fixture(params=[{}])
async def location(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
) -> AsyncGenerator[Location, None]:
    params = request.param
    item = location_factory(**params)

    del item.id

    async with async_session_maker() as async_session:
        async_session.add(item)

        await async_session.commit()
        await async_session.refresh(item)

        async_session.expunge_all()

    yield item

    delete_query = delete(Location).where(Location.id == item.id)
    # Add teardown cause fixture async_session doesn't used
    async with async_session_maker() as async_session:
        await async_session.execute(delete_query)
        await async_session.commit()


@pytest_asyncio.fixture(params=[(5, {})])
async def locations(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
) -> AsyncGenerator[list[Location], None]:
    size, params = request.param
    items = [location_factory(**params) for _ in range(size)]

    async with async_session_maker() as async_session:
        for item in items:
            del item.id
            async_session.add(item)

        await async_session.commit()
        for item in items:
            await async_session.refresh(item)

        async_session.expunge_all()

    yield items

    delete_query = delete(Location).where(Location.id.in_([item.id for item in items]))
    # Add teardown cause fixture async_session doesn't used
    async with async_session_maker() as async_session:
        await async_session.execute(delete_query)
        await async_session.commit()
