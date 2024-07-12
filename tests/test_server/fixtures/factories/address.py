from collections.abc import AsyncGenerator
from random import randint
from typing import AsyncContextManager, Callable

import pytest
import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Address, Location
from tests.test_server.fixtures.factories.base import random_string


def address_factory(**kwargs):
    data = {
        "id": randint(0, 10000000),
        "location_id": randint(0, 10000000),
        "url": random_string(32),
    }
    data.update(kwargs)
    return Address(**data)


@pytest_asyncio.fixture(params=[{}])
async def address(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    location: Location,
) -> AsyncGenerator[Address, None]:
    params = request.param
    item = address_factory(location_id=location.id, **params)

    del item.id

    async with async_session_maker() as async_session:
        async_session.add(item)
        # this is not required for backend tests, but needed by client tests
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        address_id = item.id
        await async_session.refresh(item)
        async_session.expunge(item)

    yield item

    query = delete(Address).where(Address.id == address_id)
    # Add teardown cause fixture async_session doesn't used
    async with async_session_maker() as async_session:
        await async_session.execute(query)
        await async_session.commit()


@pytest_asyncio.fixture(params=[(2, {})])
async def addresses(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    locations: list[Location],
) -> AsyncGenerator[list[Address], None]:
    size, params = request.param
    items = [address_factory(location_id=location.id, **params) for _ in range(size) for location in locations]

    async with async_session_maker() as async_session:
        for item in items:
            del item.id
            async_session.add(item)
        # this is not required for backend tests, but needed by client tests
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        for item in items:
            await async_session.refresh(item)
            async_session.expunge(item)

    yield items
