from collections.abc import AsyncGenerator
from random import choice, randint
from typing import AsyncContextManager, Callable

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Location
from tests.test_server.fixtures.factories.address import create_address
from tests.test_server.fixtures.factories.base import random_string
from tests.test_server.utils.delete import clean_db


def location_factory(**kwargs):
    data = {
        "id": randint(0, 10000000),
        "type": choice(["kafka", "postgres", "hdfs"]),
        "name": random_string(),
        "external_id": random_string(),
    }
    data.update(kwargs)
    return Location(**data)


async def create_location(
    async_session: AsyncSession,
    location_kwargs: dict | None = None,
    address_kwargs: dict | None = None,
) -> Location:
    location_kwargs = location_kwargs or {}
    location = location_factory(**location_kwargs)
    del location.id
    async_session.add(location)
    await async_session.commit()
    if address_kwargs:
        if urls := address_kwargs.pop("urls", None):
            for url in urls:
                address_kwargs["url"] = url
                await create_address(async_session, location.id, address_kwargs)
        else:
            await create_address(async_session, location.id, address_kwargs)
    await async_session.refresh(location)
    return location


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

    async with async_session_maker() as async_session:
        item = await create_location(async_session, location_kwargs=params)
        async_session.expunge_all()

    yield item

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture(params=[(5, {})])
async def locations(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
) -> AsyncGenerator[list[Location], None]:
    size, params = request.param
    async with async_session_maker() as async_session:
        items = [await create_location(async_session, location_kwargs=params) for _ in range(size)]
        async_session.expunge_all()

    yield items

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture
async def locations_search(
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
):
    """
    Fixture with explicit location name and address urls for search tests.
    """
    options = [
        {
            "location_kwargs": {
                "name": "postgres.public.users",
                "type": "postges",
            },
            "address_kwargs": {"url": "http://my-postgres-host:8012"},
        },
        {
            "location_kwargs": {
                "name": "my-cluster",
                "type": "hive",
            },
            "address_kwargs": {"url": "hdfs://my-cluster-namenode:8020"},
        },
        {
            "location_kwargs": {
                "name": "warehouse",
                "type": "hdfs",
            },
            "address_kwargs": {"url": "hdfs://warehouse-cluster-namenode:2080"},
        },
    ]
    address_urls = [option["address_kwargs"]["url"] for option in options]
    async with async_session_maker() as async_session:
        locations = [
            await create_location(
                async_session=async_session,
                location_kwargs=option["location_kwargs"],
                address_kwargs=option["address_kwargs"],
            )
            for option in options
        ]
        async_session.expunge_all()

    location_by_name = {location.name: location for location in locations}
    location_by_address = dict(zip(address_urls, locations))

    yield location_by_name, location_by_address

    async with async_session_maker() as async_session:
        await clean_db(async_session)
