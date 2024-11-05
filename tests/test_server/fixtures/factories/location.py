from collections.abc import AsyncGenerator
from random import choice, randint
from typing import AsyncContextManager, Callable

import pytest
import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Address, Location
from tests.test_server.fixtures.factories.address import address_factory
from tests.test_server.fixtures.factories.base import random_string


def location_factory(**kwargs):
    data = {
        "id": randint(0, 10000000),
        "type": choice(["kafka", "postgres", "hdfs"]),
        "name": random_string(),
        "external_id": random_string(),
    }
    data.update(kwargs)
    return Location(**data)


@pytest_asyncio.fixture(params=[{}])
async def new_location(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[Location, None]:
    params = request.param
    item = location_factory(**params)

    yield item


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


@pytest_asyncio.fixture(params=[{}])
async def location_with_address(
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

        address = address_factory(location_id=item.id)

        async_session.add(address)
        await async_session.commit()
        await async_session.refresh(address)

        async_session.expunge_all()

    yield item

    location_delete_query = delete(Location).where(Location.id == item.id)
    address_delete_query = delete(Address).where(Address.id == item.id)
    # Add teardown cause fixture async_session doesn't used
    async with async_session_maker() as async_session:
        await async_session.execute(address_delete_query)
        await async_session.execute(location_delete_query)
        await async_session.commit()


@pytest_asyncio.fixture(params=[{}])
async def locations_search(
    request: pytest.FixtureRequest,
    async_session: AsyncSession,
):
    """
    Fixture with explicit location name and address urls for search tests.
    """

    request.param
    location_names_types = [
        ("postgres.public.users", "postgres"),
        ("my-cluster", "hive"),
        ("warehouse", "hdfs"),
    ]
    locations = [location_factory(name=name, type=location_type) for name, location_type in location_names_types]

    for item in locations:
        del item.id
        async_session.add(item)

    await async_session.flush()

    addresses_url = [
        "http://my-postgres-host:8012",
        "hdfs://my-cluster-namenode:8020",
        "hdfs://warehouse-cluster-namenode:2080",
    ]

    addresses = [address_factory(location_id=location.id, url=url) for location, url in zip(locations, addresses_url)]

    for item in addresses:
        del item.id
        async_session.add(item)
    await async_session.flush()

    await async_session.commit()

    for item in locations + addresses:
        await async_session.refresh(item)
        async_session.expunge(item)

    location_by_name = {location.name: location for location in locations}
    location_by_address = {name: location for name, location in zip(addresses_url, locations)}

    yield location_by_name, location_by_address
