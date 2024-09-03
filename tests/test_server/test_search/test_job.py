from http import HTTPStatus

import pytest
from deepdiff import DeepDiff
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from data_rentgen.db.models import Job, Location

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_job_search_no_query(test_client: AsyncClient) -> None:
    response = await test_client.get("/v1/jobs/search")
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "detail": [{"input": None, "loc": ["query", "search_query"], "msg": "Field required", "type": "missing"}],
    }


async def test_job_search_in_addres_url(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_search: dict[str, Job],
) -> None:
    # Job with id 8 has two addresses urls: [http://airflow-host:2080, http://airflow-host:8020] and random name
    job = jobs_search["http://airflow-host:2080"]
    query = (
        select(Location)
        .join(Job, Job.location_id == Location.id)
        .options(selectinload(Location.addresses))
        .where(Job.id == job.id)
    )
    location = await async_session.scalar(query)

    response = await test_client.get(
        "/v1/jobs/search",
        params={"search_query": "airflow-host"},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "items": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "location": {
                    "name": location.name,
                    "type": location.type,
                    "addresses": [{"url": address.url} for address in location.addresses],
                },
            },
        ],
        "meta": {
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "pages_count": 1,
            "previous_page": None,
            "total_count": 1,
        },
    }


async def test_job_search_in_location_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_search: dict[str, Job],
) -> None:
    # Job with id 5 has location names `data-product-host`

    job = jobs_search["data-product-host"]
    query = select(Job).where(Job.id.in_([job.id])).options(selectinload(Job.location).selectinload(Location.addresses))
    result = await async_session.scalars(query)
    expected_response = {
        "items": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "location": {
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                },
            }
            for job in result.all()
        ],
        "meta": {
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "pages_count": 1,
            "previous_page": None,
            "total_count": 1,
        },
    }

    response = await test_client.get(
        "/v1/jobs/search",
        params={"search_query": "data-product"},
    )

    assert response.status_code == HTTPStatus.OK

    response_diff = DeepDiff(expected_response, response.json(), ignore_order=True)
    assert not response_diff, f"Response diff: {response_diff.to_json()}"


async def test_job_search_in_job_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_search: dict[str, Job],
) -> None:
    # Jobs with ids 0 and 2 has job name `airflow-task` and `airflow-task`
    # Jobs with id 8 has two addresses urls: [http://airflow-host:2080, http://airflow-host:8020]
    jobs = [jobs_search["airflow-task"], jobs_search["airflow-dag"], jobs_search["http://airflow-host:8020"]]
    query = (
        select(Job)
        .where(Job.id.in_([job.id for job in jobs]))
        .options(selectinload(Job.location).selectinload(Location.addresses))
    )
    result = await async_session.scalars(query)
    expected_response = {
        "items": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "location": {
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                },
            }
            for job in result.all()
        ],
        "meta": {
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "pages_count": 1,
            "previous_page": None,
            "total_count": 3,
        },
    }

    response = await test_client.get(
        "/v1/jobs/search",
        params={"search_query": "airflow"},
    )

    # At this case the order is unstable
    response_diff = DeepDiff(expected_response, response.json(), ignore_order=True)
    assert not response_diff, f"Response diff: {response_diff.to_json()}"


async def test_job_search_in_location_name_and_address_url(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_search: dict[str, Job],
) -> None:
    # Job with id 4 has location name `my-cluster`
    # Job with id 7 has address url `http://my_cluster:2080`
    jobs = [jobs_search["my-cluster"], jobs_search["http://my_cluster:2080"]]
    query = (
        select(Job)
        .where(Job.id.in_([job.id for job in jobs]))
        .options(selectinload(Job.location).selectinload(Location.addresses))
    )
    result = await async_session.scalars(query)
    expected_response = {
        "items": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "location": {
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                },
            }
            for job in result.all()
        ],
        "meta": {
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "pages_count": 1,
            "previous_page": None,
            "total_count": 2,
        },
    }

    response = await test_client.get(
        "/v1/jobs/search",
        params={"search_query": "my-cluster"},
    )

    assert response.status_code == HTTPStatus.OK

    # At this case the order is unstable
    response_diff = DeepDiff(expected_response, response.json(), ignore_order=True)
    assert not response_diff, f"Response diff: {response_diff.to_json()}"
