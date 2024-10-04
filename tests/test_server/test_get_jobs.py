from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from data_rentgen.db.models import Job, Location
from tests.test_server.utils.enrich import enrich_jobs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_jobs_by_unknown_id(
    test_client: AsyncClient,
    new_job: Job,
):
    response = await test_client.get(
        "v1/jobs",
        params={"job_id": new_job.id},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
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
    jobs = await enrich_jobs([job], async_session)
    job = jobs[0]

    response = await test_client.get(
        "v1/jobs",
        params={"job_id": job.id},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
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
                "type": job.type,
                "location": {
                    "type": job.location.type,
                    "name": job.location.name,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
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
    selected_jobs = await enrich_jobs(jobs[:2], async_session)

    response = await test_client.get(
        "v1/jobs",
        params={"job_id": [job.id for job in selected_jobs]},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
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
                "type": job.type,
                "location": {
                    "type": job.location.type,
                    "name": job.location.name,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                },
            }
            for job in sorted(selected_jobs, key=lambda x: x.name)
        ],
    }


async def test_get_jobs_no_filters(
    test_client: AsyncClient,
    jobs: list[Job],
    async_session: AsyncSession,
):
    jobs = await enrich_jobs(jobs, async_session)
    response = await test_client.get("v1/jobs")

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(jobs),
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
                "type": job.type,
                "location": {
                    "type": job.location.type,
                    "name": job.location.name,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                },
            }
            for job in sorted(jobs, key=lambda x: x.name)
        ],
    }
