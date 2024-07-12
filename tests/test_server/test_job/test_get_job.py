from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql import select

from data_rentgen.db.models import Address, Job, Location

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_job_empty(
    test_client: AsyncClient,
):
    response = await test_client.get("v1/job")

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY


async def test_get_job_missing(
    test_client: AsyncClient,
    new_job: Job,
):
    response = await test_client.get(
        f"v1/job?job_id={new_job.id}",
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": 0,
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [],
    }


async def test_get_job(
    test_client: AsyncClient,
    job: Job,
    async_session_maker: async_sessionmaker[AsyncSession],
):
    response = await test_client.get(
        f"v1/job?job_id={job.id}",
    )
    session: AsyncSession = async_session_maker()
    raw_location = await session.scalar(select(Location).where(Location.id == job.location_id))
    raw_addresses = await session.scalar(select(Address).where(Address.location_id == job.location_id))
    location = {
        "addresses": [
            {"url": raw_addresses.url},
        ],
        "name": raw_location.name,
        "type": raw_location.type,
    }

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": 1,
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [
            {
                "id": job.id,
                "location": location,
                "name": job.name,
            },
        ],
    }


async def test_get_jobs(
    test_client: AsyncClient,
    jobs: list[Job],
    async_session_maker: async_sessionmaker[AsyncSession],
):
    jobs = sorted(jobs, key=lambda js: js.id)
    response = await test_client.get(
        f"v1/job?job_id={jobs[0].id}&job_id={jobs[1].id}",
    )
    session: AsyncSession = async_session_maker()
    for job in jobs:
        raw_location = await session.scalar(select(Location).where(Location.id == job.location_id))
        raw_addresses = await session.scalars(select(Address).where(Address.location_id == job.location_id))
        job.location = {
            "addresses": [{"url": address.url} for address in raw_addresses.all()],
            "name": raw_location.name,
            "type": raw_location.type,
        }

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": 2,
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [
            {
                "id": jobs[0].id,
                "location": jobs[0].location,
                "name": jobs[0].name,
            },
            {
                "id": jobs[1].id,
                "location": jobs[1].location,
                "name": jobs[1].name,
            },
        ],
    }
