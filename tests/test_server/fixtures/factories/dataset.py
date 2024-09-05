from collections.abc import AsyncGenerator
from random import choice, randint
from typing import AsyncContextManager, Callable

import pytest
import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Address, Dataset, Location
from tests.test_server.fixtures.factories.address import address_factory
from tests.test_server.fixtures.factories.base import random_string
from tests.test_server.fixtures.factories.location import location_factory


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

        await async_session.commit()
        await async_session.refresh(item)

        async_session.expunge_all()

    yield item

    delete_query = delete(Dataset).where(Dataset.id == item.id)
    # Add teardown cause fixture async_session doesn't used
    async with async_session_maker() as async_session:
        await async_session.execute(delete_query)
        await async_session.commit()


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

        await async_session.commit()
        for item in items:
            await async_session.refresh(item)

        async_session.expunge_all()

    yield items

    delete_query = delete(Dataset).where(Dataset.id.in_([item.id for item in items]))
    # Add teardown cause fixture async_session doesn't used
    async with async_session_maker() as async_session:
        await async_session.execute(delete_query)
        await async_session.commit()


@pytest_asyncio.fixture(params=[{}])
async def datasets_search(
    request: pytest.FixtureRequest,
    async_session: AsyncSession,
) -> AsyncGenerator[dict[str, Dataset], None]:
    """
    Fixture with explicit dataset, locations names and addresses urls for search tests.
    The fixtures create database structure like this:
    |Dataset ID | Dataset.name                       | Location.name               | Location.type | Address.url                        |
    |---------- | ---------------------------------- | --------------------------- | ------------- | ---------------------------------- |
    |0          | 'postgres.public.history'          | 'random-location-name'      | 'random'      | 'random-url'                       |
    |0          | 'postgres.public.history'          | 'random-location-name'      | 'random'      | 'random-url'                       |
    |1          | 'postgres.public.location_history' | 'random-location-name'      | 'random'      | 'random-url'                       |
    |1          | 'postgres.public.location_history' | 'random-location-name'      | 'random'      | 'random-url'                       |
    |2          | '/user/hive/warehouse/transfers'   | 'random-location-name'      | 'random'      | 'random-url'                       |
    |2          | '/user/hive/warehouse/transfers'   | 'random-location-name'      | 'random'      | 'random-url'                       |
    |3          | 'random-dataset-name'              | 'postgres.location'         | 'postgres'    | 'random-url'                       |
    |3          | 'random-dataset-name'              | 'postgres.location'         | 'postgres'    | 'random-url'                       |
    |4          | 'random-dataset-name'              | 'postgres.history_location' | 'postgres'    | 'random-url'                       |
    |4          | 'random-dataset-name'              | 'postgres.history_location' | 'postgres'    | 'random-url'                       |
    |5          | 'random-dataset-name'              | 'my-cluster'                | 'hdfs'        | 'random-url'                       |
    |5          | 'random-dataset-name'              | 'my-cluster'                | 'hdfs'        | 'random-url'                       |
    |6          | 'random-dataset-name'              | 'random-location-name'      | 'random'      | 'http://my-postgres-host:8012'     |
    |6          | 'random-dataset-name'              | 'random-location-name'      | 'random'      | 'http://my-postgres-host:2108'     |
    |7          | 'random-dataset-name'              | 'random-location-name'      | 'random'      | 'http://your-postgres-host:2108'   |
    |7          | 'random-dataset-name'              | 'random-location-name'      | 'random'      | 'http://your-postgres-host:8012'   |
    |8          | 'random-dataset-name'              | 'random-location-name'      | 'random'      | 'hdfs://my-cluster-namenode:2080'  |
    |8          | 'random-dataset-name'              | 'random-location-name'      | 'random'      | 'hdfs://my-cluster-namenode:8020'  |

    Every location relate to two dataset and two addresses. 2-1-2
    tip: you can imagine it like identity matrix with not-random names on diagonal.
    """
    request.param
    location_names_types = [
        ("postgres.location", "postgres"),
        ("postgres.history_location", "postgres"),
        ("my-cluster", "hdfs"),
    ]
    locations_with_names = [
        location_factory(name=name, type=location_type) for name, location_type in location_names_types
    ]
    locations_with_random_name = [location_factory(type="random") for _ in range(6)]
    locations = locations_with_names + locations_with_random_name

    for item in locations:
        del item.id
        async_session.add(item)

    await async_session.flush()

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
    for item in addresses:
        del item.id
        async_session.add(item)
    await async_session.flush()

    datasets_names = ["postgres.public.history", "postgres.public.location_history", "/user/hive/warehouse/transfers"]
    datasets_with_name = [
        dataset_factory(name=name, location_id=location.id)
        for name, location in zip(datasets_names, locations_with_random_name[3:])
    ]
    datasets_with_random_name = [dataset_factory(location_id=location.id) for location in locations[:6]]
    datasets = datasets_with_name + datasets_with_random_name
    for item in datasets:
        del item.id
        async_session.add(item)

    await async_session.commit()

    for item in locations + addresses + datasets:
        await async_session.refresh(item)
        async_session.expunge(item)

    entities = {name: dataset for name, dataset in zip(datasets_names, datasets[:3])}
    entities.update(
        {
            name: dataset
            for name, dataset in zip(
                [name for name, _ in location_names_types],
                datasets[3:6],
            )
        },
    )
    entities.update(
        {
            name: dataset
            for name, dataset in zip(addresses_url, [dataset for dataset in datasets[6:] for _ in range(2)])
        },
    )

    yield entities
