from collections.abc import AsyncGenerator
from random import choice, randint
from typing import AsyncContextManager, Callable

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Address, Dataset
from tests.test_server.fixtures.factories.base import random_string


def dataset_factory(**kwargs):
    data = {
        "id": randint(0, 10000000),
        "location_id": randint(0, 10000000),
        "name": random_string(32),
    }

    data.update(kwargs)
    return Dataset(**data)


@pytest_asyncio.fixture(params=[{}])
async def new_dataset(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[Dataset, None]:
    params = request.param
    item = dataset_factory(**params)

    yield item


@pytest_asyncio.fixture(params=[{}])
async def dataset(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    address: Address,
) -> AsyncGenerator[Dataset, None]:
    params = request.param
    item = dataset_factory(location_id=address.location_id, **params)
    del item.id

    async with async_session_maker() as async_session:
        async_session.add(item)
        # this is not required for backend tests, but needed by client tests
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        await async_session.refresh(item)
        async_session.expunge(item)

    yield item


@pytest_asyncio.fixture(params=[(2, {})])
async def datasets(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    addresses: list[Address],
) -> AsyncGenerator[list[Dataset], None]:
    size, params = request.param
    items = [dataset_factory(location_id=choice(addresses).location_id, **params) for _ in range(size)]

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


@pytest_asyncio.fixture(params=[{}])
async def datasets_with_clear_name(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    addresses: list[Address],
) -> AsyncGenerator[list[Dataset], None]:
    params = request.param
    names = ["postgres.public.history", "postgres.public.location_history", "/user/hive/warehouse/transfers"]
    items = [dataset_factory(location_id=choice(addresses).location_id, name=name, **params) for name in names]

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
