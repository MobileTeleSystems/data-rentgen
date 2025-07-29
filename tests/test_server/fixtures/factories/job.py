from __future__ import annotations

from random import randint
from typing import TYPE_CHECKING

import pytest_asyncio

from data_rentgen.db.models import Job
from tests.test_server.fixtures.factories.base import random_string
from tests.test_server.fixtures.factories.job_type import create_job_type
from tests.test_server.fixtures.factories.location import create_location
from tests.test_server.utils.delete import clean_db

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable
    from contextlib import AbstractAsyncContextManager

    import pytest
    from sqlalchemy.ext.asyncio import AsyncSession


def job_factory(**kwargs):
    data = {
        "id": randint(0, 10000000),
        "location_id": randint(0, 10000000),
        "name": random_string(32),
        "type_id": randint(0, 10000000),
    }
    data.update(kwargs)
    return Job(**data)


async def create_job(
    async_session: AsyncSession,
    location_id: int,
    job_type_id: int,
    job_kwargs: dict | None = None,
) -> Job:
    if job_kwargs:
        job_kwargs.update({"location_id": location_id, "type_id": job_type_id})
    else:
        job_kwargs = {"location_id": location_id, "type_id": job_type_id}
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
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
) -> AsyncGenerator[Job, None]:
    params = request.param

    async with async_session_maker() as async_session:
        location = await create_location(async_session)
        job_type = await create_job_type(async_session)
        item = await create_job(async_session, location_id=location.id, job_type_id=job_type.id, job_kwargs=params)

        async_session.expunge_all()

    yield item

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture(params=[(5, {})])
async def jobs(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
) -> AsyncGenerator[list[Job], None]:
    size, params = request.param

    async with async_session_maker() as async_session:
        items = []
        for _ in range(size):
            location = await create_location(async_session)
            job_type = await create_job_type(async_session)
            item = await create_job(async_session, location_id=location.id, job_type_id=job_type.id, **params)
            items.append(item)

        async_session.expunge_all()

    yield items

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture
async def jobs_search(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
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
    location_kwargs = [
        {"name": "dwh", "type": "yarn"},
        {"name": "my-cluster", "type": "yarn"},
        {"name": "data-product-host", "type": "http"},
    ]
    address_kwargs = [
        {"urls": ["yarn://my_cluster_1", "yarn://my_cluster_2"]},
        {"urls": ["http://some.host.name:8020", "http://some.host.name:2080"]},
        {"urls": ["http://airflow-host:8020", "http://airflow-host:2080"]},
    ]
    jobs_kwargs = [
        {"name": "airflow-task"},
        {"name": "spark_application"},
        {"name": "airflow-dag"},
    ]
    addresses_url = [url for address in address_kwargs for url in address["urls"]]
    async with async_session_maker() as async_session:
        locations_with_names = [
            await create_location(
                async_session,
                location_kwargs=kwargs,
                address_kwargs={"urls": [random_string(32), random_string(32)]},  # Each location has 2 addresses
            )
            for kwargs in location_kwargs
        ]
        locations_with_address_urls = [
            await create_location(
                async_session,
                address_kwargs=kwargs,
            )
            for kwargs in address_kwargs
        ]
        random_location = [
            await create_location(
                async_session,
                location_kwargs={"type": "random"},
                address_kwargs={"urls": [random_string(32), random_string(32)]},
            )
            for _ in range(3)
        ]

        job_type = await create_job_type(async_session)

        jobs_with_names = [
            await create_job(
                async_session,
                location_id=random_location[i].id,
                job_type_id=job_type.id,
                job_kwargs=kwargs,
            )
            for i, kwargs in enumerate(jobs_kwargs)
        ]
        jobs_with_location_names = [
            await create_job(
                async_session,
                location_id=location.id,
                job_type_id=job_type.id,
            )
            for location in locations_with_names
        ]
        jobs_with_address_urls = [
            await create_job(
                async_session,
                location_id=location.id,
                job_type_id=job_type.id,
            )
            for location in locations_with_address_urls
        ]

        async_session.expunge_all()

    jobs_by_name = {job.name: job for job in jobs_with_names}
    jobs_by_location = dict(
        zip([location.name for location in locations_with_names], jobs_with_location_names, strict=False),
    )
    jobs_by_address = dict(zip(addresses_url, [job for job in jobs_with_address_urls for _ in range(2)], strict=False))

    yield jobs_by_name, jobs_by_location, jobs_by_address

    async with async_session_maker() as async_session:
        await clean_db(async_session)
