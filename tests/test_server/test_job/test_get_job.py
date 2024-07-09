from http import HTTPStatus

import pytest
from httpx import AsyncClient

from data_rentgen.db.models import Job

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_job_empty(
    test_client: AsyncClient,
):
    response = await test_client.get("v1/job")

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
):
    response = await test_client.get(
        f"v1/job?job_id={job.id}",
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
                "id": job.id,
                "location_id": job.location_id,
                "name": job.name,
            },
        ],
    }


async def test_get_jobs(
    test_client: AsyncClient,
    jobs: list[Job],
):
    jobs = sorted(jobs, key=lambda js: js.id)
    response = await test_client.get(
        f"v1/job?job_id={jobs[0].id}&job_id={jobs[1].id}",
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
                "id": jobs[0].id,
                "location_id": jobs[0].location_id,
                "name": jobs[0].name,
            },
            {
                "id": jobs[1].id,
                "location_id": jobs[1].location_id,
                "name": jobs[1].name,
            },
        ],
    }
