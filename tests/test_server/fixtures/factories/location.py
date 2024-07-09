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
        # this is not required for backend tests, but needed by client tests
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        location_id = item.id
        await async_session.refresh(item)
        async_session.expunge(item)

    yield item

    query = delete(Location).where(Location.id == location_id)
    async with async_session_maker() as async_session:
        await async_session.execute(query)
        await async_session.commit()
