from collections.abc import AsyncGenerator
from random import choice, randint
from typing import AsyncContextManager, Callable

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Address, Dataset, Location
from tests.test_server.fixtures.factories.address import address_factory
from tests.test_server.fixtures.factories.base import random_string
from tests.test_server.fixtures.factories.location import location_factory

dataset_search_fixture_annotation = tuple[list[Address], list[Location], list[Dataset]]


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
async def datasets_search(
    request: pytest.FixtureRequest,
    async_session: AsyncSession,
) -> AsyncGenerator[tuple[list[Address], list[Location], list[Dataset]], None]:
    """
    Fixture with explicit dataset, locations and addresses names for search tests.
    The fixtures create database structure like this:
    | Dataset.name                   | Location.name    | Address.url                     |
    | ------------------------------ | ---------------- | ------------------------------- |
    | random_name                    | same_random_name | hdfs://my-cluster-namenode:8020 |
    | random_name                    | same_random_name | hdfs://my-cluster-namenode:2080 |
    | ...                            | ...              | ...                             |
    | random                         | my-cluster       | random                          |
    | ...                            | ...              | ...                             |
    | /user/hive/warehouse/transfers | same_random_name | same_random_url                 |
    | /user/hive/warehouse/transfers | same_random_name | same_random_url                 |

    Every location relate to two dataset and two addresses. 2-1-2
    tip: you can imagine it like identity matrix with not-random names on diagonal.
    """
    request.param

    location_names = ["postgres.location", "postgres.history_location", "my-cluster"]
    locations_with_names = [
        location_factory(name=name, type=location_type)
        for name, location_type in zip(location_names, ["postgres", "postgres", "hdfs"])
    ]
    locations_with_random_name = [location_factory() for _ in range(6)]
    locations = locations_with_names + locations_with_random_name

    for item in locations:
        del item.id
        async_session.add(item)
    # this is not required for backend tests, but needed by client tests
    await async_session.commit()

    # remove current object from async_session. this is required to compare object against new state fetched
    # from database, and also to remove it from cache
    for item in locations:
        await async_session.refresh(item)
        async_session.expunge(item)

    addresses_url = (
        ["http://my-postgres-host:8012", "http://my-postgres-host:2108"]
        + ["http://your-postgres-host:8012", "http://your-postgres-host:2108"]
        + ["hdfs://my-cluster-namenode:8020", "hdfs://my-cluster-namenode:2080"]
    )
    # Each location has 2 addresses
    addresses_with_name = [
        address_factory(url=url, location_id=location.id)
        for url, location in zip(
            addresses_url,
            [location for location in locations_with_random_name[:3] for _ in range(2)],
        )
    ]
    addresses_with_random_name = [
        address_factory(location_id=location.id) for location in (locations[:3] + locations[6:]) * 2
    ]
    addresses = addresses_with_name + addresses_with_random_name

    datasets_names = ["postgres.public.history", "postgres.public.location_history", "/user/hive/warehouse/transfers"]
    datasets_with_name = [
        dataset_factory(name=name, location_id=location.id)
        for name, location in zip(datasets_names, locations_with_random_name[3:])
    ]
    datasets_with_random_name = [dataset_factory(location_id=location.id) for location in locations[:6]]
    datasets = datasets_with_name + datasets_with_random_name

    for item in addresses + datasets:
        del item.id
        async_session.add(item)
    # this is not required for backend tests, but needed by client tests
    await async_session.commit()

    # remove current object from async_session. this is required to compare object against new state fetched
    # from database, and also to remove it from cache
    for item in addresses + datasets:
        await async_session.refresh(item)
        async_session.expunge(item)

    yield addresses, locations, datasets
