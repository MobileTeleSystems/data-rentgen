from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Job
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.enrich import enrich_jobs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_jobs_by_unknown_id(
    test_client: AsyncClient,
    new_job: Job,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
    mocked_user: MockedUser,
):
    jobs = await enrich_jobs([job], async_session)
    job = jobs[0]

    response = await test_client.get(
        "v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                "data": {
                    "kind": "JOB",
                    "id": job.id,
                    "name": job.name,
                    "type": job.type,
                    "location": {
                        "id": job.location.id,
                        "type": job.location.type,
                        "name": job.location.name,
                        "addresses": [{"url": address.url} for address in job.location.addresses],
                        "external_id": job.location.external_id,
                    },
                },
            },
        ],
    }


async def test_get_jobs_by_multiple_ids(
    test_client: AsyncClient,
    jobs: list[Job],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    # create more objects than pass to endpoint, to test filtering
    selected_jobs = await enrich_jobs(jobs[:2], async_session)

    response = await test_client.get(
        "v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                "data": {
                    "kind": "JOB",
                    "id": job.id,
                    "name": job.name,
                    "type": job.type,
                    "location": {
                        "id": job.location.id,
                        "type": job.location.type,
                        "name": job.location.name,
                        "addresses": [{"url": address.url} for address in job.location.addresses],
                        "external_id": job.location.external_id,
                    },
                },
            }
            for job in sorted(selected_jobs, key=lambda x: x.name)
        ],
    }
