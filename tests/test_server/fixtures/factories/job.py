from collections.abc import AsyncGenerator
from random import choice, randint
from typing import AsyncContextManager, Callable

import pytest
import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Address, Job, JobType
from tests.test_server.fixtures.factories.address import address_factory
from tests.test_server.fixtures.factories.base import random_string
from tests.test_server.fixtures.factories.location import (
    create_location,
    location_factory,
)


def job_factory(**kwargs):
    data = {
        "id": randint(0, 10000000),
        "location_id": randint(0, 10000000),
        "name": random_string(32),
        "type": choice(list(JobType)),
    }
    data.update(kwargs)
    return Job(**data)


async def create_job(
    async_session: AsyncSession,
    job_kwargs: dict | None = None,
    location_kwargs: dict | None = None,
) -> Job:
    location = await create_location(async_session, location_kwargs)
    if job_kwargs:
        job_kwargs.update({"location_id": location.id})
    else:
        job_kwargs = {"location_id": location.id}
    job = job_factory(**job_kwargs)
    del job.id
    async_session.add(job)
    await async_session.commit()
    await async_session.refresh(job)
    return job


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

        await async_session.commit()
        await async_session.refresh(item)

        async_session.expunge_all()

    yield item

    delete_query = delete(Job).where(Job.id == item.id)
    # Add teardown cause fixture async_session doesn't used
    async with async_session_maker() as async_session:
        await async_session.execute(delete_query)
        await async_session.commit()


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

        await async_session.commit()
        for item in items:
            await async_session.refresh(item)

        async_session.expunge_all()

    yield items

    delete_query = delete(Job).where(Job.id.in_([item.id for item in items]))
    # Add teardown cause fixture async_session doesn't used
    async with async_session_maker() as async_session:
        await async_session.execute(delete_query)
        await async_session.commit()


@pytest_asyncio.fixture(params=[{}])
async def jobs_search(
    request: pytest.FixtureRequest,
    async_session: AsyncSession,
) -> AsyncGenerator[tuple[dict[str, Job], dict[str, Job], dict[str, Job]], None]:
    """
    Fixture with explicit jobs, locations names and addresses urls for search tests.
    The fixtures create database structure like this:
    |Job ID     | Job.name                           | Location.name               | Location.type | Address.url                        |
    |---------- | ---------------------------------- | --------------------------- | ------------- | ---------------------------------- |
    |0          | 'airflow-task'                     | 'random-location-name'      | 'random'      | 'random-url'                       |
    |0          | 'airflow-task'                     | 'random-location-name'      | 'random'      | 'random-url'                       |
    |1          | 'spark-application'                | 'random-location-name'      | 'random'      | 'random-url'                       |
    |1          | 'spark-application'                | 'random-location-name'      | 'random'      | 'random-url'                       |
    |2          | 'airflow-dag'                      | 'random-location-name'      | 'random'      | 'random-url'                       |
    |2          | 'airflow-dag'                      | 'random-location-name'      | 'random'      | 'random-url'                       |
    |3          | 'random-job-name'                  | 'dwh'                       | 'yarn'        | 'random-url'                       |
    |3          | 'random-job-name'                  | 'dwh'                       | 'yarn'        | 'random-url'                       |
    |4          | 'random-job-name'                  | 'my-cluster'                | 'yarn'        | 'random-url'                       |
    |4          | 'random-job-name'                  | 'my-cluster'                | 'yarn'        | 'random-url'                       |
    |5          | 'random-job-name'                  | 'data-product-host'         | 'http'        | 'random-url'                       |
    |5          | 'random-job-name'                  | 'data-product-host'         | 'http'        | 'random-url'                       |
    |6          | 'random-job-name'                  | 'random-location-name'      | 'random'      | 'yarn://my_cluster_1'              |
    |6          | 'random-job-name'                  | 'random-location-name'      | 'random'      | 'yarn://my_cluster_2'              |
    |7          | 'random-job-name'                  | 'random-location-name'      | 'random'      | 'http://some.host.name:2080'       |
    |7          | 'random-job-name'                  | 'random-location-name'      | 'random'      | 'http://some.host.name:8020'       |
    |8          | 'random-job-name'                  | 'random-location-name'      | 'random'      | 'http://airflow-host:8020'         |
    |8          | 'random-job-name'                  | 'random-location-name'      | 'random'      | 'http://airflow-host:2080'         |

    tip: you can imagine it like identity matrix with not-random names on diagonal.
    """
    request.param
    location_names_types = [("dwh", "yarn"), ("my-cluster", "yarn"), ("data-product-host", "http")]
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
        ["yarn://my_cluster_1", "yarn://my_cluster_2"]
        + ["http://some.host.name:8020", "http://some.host.name:2080"]
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
    for item in addresses:
        del item.id
        async_session.add(item)
    await async_session.flush()

    jobs_names = ["airflow-task", "spark_application", "airflow-dag"]
    jobs_with_name = [
        job_factory(name=name, location_id=location.id)
        for name, location in zip(jobs_names, locations_with_random_name[3:])
    ]
    jobs_with_random_name = [job_factory(location_id=location.id) for location in locations[:6]]
    jobs = jobs_with_name + jobs_with_random_name
    for item in jobs:
        del item.id
        async_session.add(item)
    await async_session.commit()

    for item in locations + addresses + jobs:
        await async_session.refresh(item)
        async_session.expunge(item)

    jobs_by_name = {name: job for name, job in zip(jobs_names, jobs[:3])}
    jobs_by_location = {
        name: job
        for name, job in zip(
            [name for name, _ in location_names_types],
            jobs[3:6],
        )
    }
    jobs_by_address = {name: job for name, job in zip(addresses_url, [job for job in jobs[6:] for _ in range(2)])}

    yield jobs_by_name, jobs_by_location, jobs_by_address
