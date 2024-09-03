from collections.abc import AsyncGenerator
from random import choice, randint
from typing import AsyncContextManager, Callable

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Address, Job, JobType
from tests.test_server.fixtures.factories.address import address_factory
from tests.test_server.fixtures.factories.base import random_string
from tests.test_server.fixtures.factories.location import location_factory


def job_factory(**kwargs):
    data = {
        "id": randint(0, 10000000),
        "location_id": randint(0, 10000000),
        "name": random_string(32),
        "type": choice(list(JobType)),
    }
    data.update(kwargs)
    return Job(**data)


@pytest_asyncio.fixture(params=[{}])
async def new_job(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[Job, None]:
    params = request.param
    item = job_factory(**params)

    yield item


@pytest_asyncio.fixture(params=[{}])
async def job(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    address: Address,
) -> AsyncGenerator[Job, None]:
    params = request.param
    item = job_factory(location_id=address.location_id, **params)
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


@pytest_asyncio.fixture(params=[(5, {})])
async def jobs(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    addresses: list[Address],
) -> AsyncGenerator[list[Job], None]:
    size, params = request.param
    items = [job_factory(location_id=choice(addresses).location_id, **params) for _ in range(size)]

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
async def jobs_search(
    request: pytest.FixtureRequest,
    async_session: AsyncSession,
) -> AsyncGenerator[dict[str, Job], None]:
    """
    Fixture with explicit dataset, locations and addresses names for search tests.
    The fixtures create database structure like this:
    |Job ID     | Job.name                           | Location.name               | Location.type | Address.url                        |
    |---------- | ---------------------------------- | --------------------------- | ------------- | ---------------------------------- |
    |0          | 'spark-job'                        | 'random-location-name'      | 'kafka'       | 'random-url'                       |
    |0          | 'spark-job'                        | 'random-location-name'      | 'kafka'       | 'random-url'                       |
    |1          | 'spark-application'                | 'random-location-name'      | 'kafka'       | 'random-url'                       |
    |1          | 'spark-application'                | 'random-location-name'      | 'kafka'       | 'random-url'                       |
    |2          | 'airflow-dag'                      | 'random-location-name'      | 'kafka'       | 'random-url'                       |
    |2          | 'airflow-dag'                      | 'random-location-name'      | 'kafka'       | 'random-url'                       |
    |3          | 'random-job-name'                  | 'dwh'                       | 'yarn'        | 'random-url'                       |
    |3          | 'random-job-name'                  | 'dwh'                       | 'yarn'        | 'random-url'                       |
    |4          | 'random-job-name'                  | 'my-cluster'                | 'yarn'        | 'random-url'                       |
    |4          | 'random-job-name'                  | 'my-cluster'                | 'yarn'        | 'random-url'                       |
    |5          | 'random-job-name'                  | 'data-product:8020'         | 'http'        | 'random-url'                       |
    |5          | 'random-job-name'                  | 'data-product:8020'         | 'htpp'        | 'random-url'                       |
    |6          | 'random-job-name'                  | 'random-location-name'      | 'kafka'       | 'yarn:dwh:8012'                    |
    |6          | 'random-job-name'                  | 'random-location-name'      | 'kafka'       | 'yarn:dwh:2108'                    |
    |7          | 'random-job-name'                  | 'random-location-name'      | 'kafka'       | 'yarn:my-cluster:2108'             |
    |7          | 'random-job-name'                  | 'random-location-name'      | 'kafka'       | 'yarn:my-cluster:8012'             |
    |8          | 'random-job-name'                  | 'random-location-name'      | 'kafka'       | 'hdfs://my-cluster-namenode:2080'  |
    |8          | 'random-job-name'                  | 'random-location-name'      | 'kafka'       | 'hdfs://my-cluster-namenode:8020'  |

    Every location relate to two job and two addresses. 2-1-2
    tip: you can imagine it like identity matrix with not-random names on diagonal.
    """
    request.param
    location_names = ["dwh", "my-cluster", "data-product:8020"]
    locations_with_names = [
        location_factory(name=name, type=location_type)
        for name, location_type in zip(location_names, ["yarn", "yarn", "http"])
    ]
    locations_with_random_name = [location_factory(type="kafka") for _ in range(6)]
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
        ["yarn:dwh:8012", "yarn:dwh:2108"]
        + ["yarn:my-cluster:8012", "yarn:my-cluster:2108"]
        + ["http://airflow-host:8020", "http://airflow-host:2080"]
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

    jobs_names = ["spark_job", "spark_application", "airflow-dag"]
    jobs_with_name = [
        job_factory(name=name, location_id=location.id)
        for name, location in zip(jobs_names, locations_with_random_name[3:])
    ]
    jobs_with_random_name = [job_factory(location_id=location.id) for location in locations[:6]]
    jobs = jobs_with_name + jobs_with_random_name
    for item in addresses + jobs:
        del item.id
        async_session.add(item)
    # this is not required for backend tests, but needed by client tests
    await async_session.commit()

    # remove current object from async_session. this is required to compare object against new state fetched
    # from database, and also to remove it from cache
    for item in addresses + jobs:
        await async_session.refresh(item)
        async_session.expunge(item)

    entities = {name: job for name, job in zip(jobs_names, jobs[:3])}
    entities.update({name: job for name, job in zip(location_names, jobs[3:6])})
    entities.update(
        {name: job for name, job in zip(addresses_url, [job for job in jobs[6:] for _ in range(2)])},
    )

    yield entities
