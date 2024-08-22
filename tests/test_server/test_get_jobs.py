from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from data_rentgen.db.models import Job, Location

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_jobs_by_unknown_id(
    test_client: AsyncClient,
    new_job: Job,
):
    response = await test_client.get(
        "v1/jobs",
        params={"job_id": new_job.id},
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


async def test_get_jobs_by_one_id(
    test_client: AsyncClient,
    job: Job,
    async_session: AsyncSession,
):
    query = select(Job).where(Job.id == job.id).options(selectinload(Job.location).selectinload(Location.addresses))
    job = await async_session.scalar(query)

    response = await test_client.get(
        "v1/jobs",
        params={"job_id": job.id},
    )

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
                "kind": "JOB",
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


async def test_get_jobs_by_multiple_ids(
    test_client: AsyncClient,
    jobs: list[Job],
    async_session: AsyncSession,
):
    # create more objects than pass to endpoint, to test filtering
    job_ids = [job.id for job in jobs[:2]]

    query = (
        select(Job)
        .where(Job.id.in_(job_ids))
        .order_by(Job.name)
        .options(selectinload(Job.location).selectinload(Location.addresses))
    )
    scalars = await async_session.scalars(query)
    jobs_from_db = list(scalars.all())

    response = await test_client.get(
        "v1/jobs",
        params={"job_id": job_ids},
    )

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
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "location": {
                    "type": job.location.type,
                    "name": job.location.name,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                },
            }
            for job in jobs_from_db
        ],
    }


async def test_get_jobs_no_filters(
    test_client: AsyncClient,
    jobs: list[Job],
    async_session: AsyncSession,
):
    query = select(Job).order_by(Job.name).options(selectinload(Job.location).selectinload(Location.addresses))
    scalars = await async_session.scalars(query)
    jobs_from_db = list(scalars.all())

    response = await test_client.get("v1/jobs")

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(jobs_from_db),
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "location": {
                    "type": job.location.type,
                    "name": job.location.name,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                },
            }
            for job in jobs_from_db
        ],
    }
