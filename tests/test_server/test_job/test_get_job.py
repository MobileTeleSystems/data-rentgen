from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from data_rentgen.db.models import Job, Location

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
    async_session: AsyncSession,
):
    response = await test_client.get(
        f"v1/job?job_id={job.id}",
    )

    query = select(Job).where(Job.id == job.id).options(selectinload(Job.location).selectinload(Location.addresses))

    job = await async_session.scalar(query)

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
                "name": job.name,
                "location": {
                    "type": job.location.type,
                    "name": job.location.name,
                    "addresses": [
                        {"url": job.location.addresses[0].url},
                    ],
                },
            },
        ],
    }


async def test_get_jobs(
    test_client: AsyncClient,
    jobs: list[Job],
    async_session: AsyncSession,
):
    response = await test_client.get(
        f"v1/job?job_id={jobs[0].id}&job_id={jobs[1].id}",
    )

    query = (
        select(Job)
        .where(Job.id.in_([job.id for job in jobs]))
        .order_by(Job.id)
        .options(selectinload(Job.location).selectinload(Location.addresses))
    )

    scalars = await async_session.scalars(query)
    jobs = list(scalars.all())

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
                "id": job.id,
                "name": job.name,
                "location": {
                    "type": job.location.type,
                    "name": job.location.name,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                },
            }
            for job in jobs[:2]
        ],
    }
