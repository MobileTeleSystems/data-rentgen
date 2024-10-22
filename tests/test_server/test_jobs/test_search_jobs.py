from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Job
from tests.test_server.utils.enrich import enrich_jobs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_search_jobs_by_address_url(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_search: tuple[dict[str, Job], dict[str, Job], dict[str, Job]],
) -> None:
    _, _, jobs_by_address = jobs_search
    # Job with id 8 has two addresses urls: [http://airflow-host:2080, http://airflow-host:8020] and random name
    jobs = await enrich_jobs([jobs_by_address["http://airflow-host:2080"]], async_session)

    response = await test_client.get(
        "/v1/jobs",
        params={"search_query": "airflow-host"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
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
        "items": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            }
            for job in jobs
        ],
    }


async def test_search_jobs_by_location_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_search: tuple[dict[str, Job], dict[str, Job], dict[str, Job]],
) -> None:
    _, jobs_by_location, _ = jobs_search
    # Job with id 5 has location names `data-product-host`
    jobs = await enrich_jobs([jobs_by_location["data-product-host"]], async_session)

    response = await test_client.get(
        "/v1/jobs",
        params={"search_query": "data-product"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
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
        "items": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            }
            for job in jobs
        ],
    }


async def test_search_jobs_by_job_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_search: tuple[dict[str, Job], dict[str, Job], dict[str, Job]],
) -> None:
    jobs_by_name, _, jobs_by_address = jobs_search
    # Jobs with ids 0 and 2 has job name `airflow-task` and `airflow-task`
    # Jobs with id 8 has two addresses urls: [http://airflow-host:2080, http://airflow-host:8020]
    jobs = await enrich_jobs(
        [
            # on top of the search are results with shorter name,
            # then sorted alphabetically
            jobs_by_name["airflow-dag"],
            jobs_by_name["airflow-task"],
            jobs_by_address["http://airflow-host:8020"],
        ],
        async_session,
    )

    response = await test_client.get(
        "/v1/jobs",
        # search by word prefix
        params={"search_query": "airflo"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
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
        "items": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            }
            for job in jobs
        ],
    }


async def test_search_jobs_by_location_name_and_address_url(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_search: tuple[dict[str, Job], dict[str, Job], dict[str, Job]],
) -> None:
    _, jobs_by_location, jobs_by_address = jobs_search
    # Job with id 4 has location name `my-cluster`
    # Job with id 6 has address urls: [`yarn://my_cluster_1`, `yarn://my_cluster_2`]
    jobs = await enrich_jobs([jobs_by_location["my-cluster"], jobs_by_address["yarn://my_cluster_1"]], async_session)

    response = await test_client.get(
        "/v1/jobs",
        params={"search_query": "my-cluster"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
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
        "items": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            }
            for job in jobs
        ],
    }


async def test_search_jobs_no_results(
    test_client: AsyncClient,
    jobs_search: tuple[dict[str, Job], dict[str, Job], dict[str, Job]],
) -> None:
    response = await test_client.get(
        "/v1/jobs",
        params={"search_query": "not-found"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "pages_count": 1,
            "previous_page": None,
            "total_count": 0,
        },
        "items": [],
    }
