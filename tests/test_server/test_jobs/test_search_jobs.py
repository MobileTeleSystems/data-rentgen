from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Job, JobType
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import job_to_json
from tests.test_server.utils.enrich import enrich_jobs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_search_jobs_by_address_url(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_search: tuple[dict[str, Job], dict[str, Job], dict[str, Job]],
    mocked_user: MockedUser,
) -> None:
    _, _, jobs_by_address = jobs_search
    # Job with id 8 has two addresses urls: [http://airflow-host:2080, http://airflow-host:8020] and random name
    jobs = await enrich_jobs([jobs_by_address["http://airflow-host:2080"]], async_session)

    response = await test_client.get(
        "/v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                "id": str(job.id),
                "data": job_to_json(job),
            }
            for job in jobs
        ],
    }


async def test_search_jobs_by_location_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_search: tuple[dict[str, Job], dict[str, Job], dict[str, Job]],
    mocked_user: MockedUser,
) -> None:
    _, jobs_by_location, _ = jobs_search
    # Job with id 5 has location names `data-product-host`
    jobs = await enrich_jobs([jobs_by_location["data-product-host"]], async_session)

    response = await test_client.get(
        "/v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                "id": str(job.id),
                "data": job_to_json(job),
            }
            for job in jobs
        ],
    }


async def test_search_jobs_by_location_external_id(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_search: tuple[dict[str, Job], dict[str, Job], dict[str, Job]],
    mocked_user: MockedUser,
) -> None:
    _, jobs_by_location, _ = jobs_search
    # Job with id 6 has location with external_id: "abc123"
    jobs = await enrich_jobs([jobs_by_location["with-external-id"]], async_session)

    response = await test_client.get(
        "/v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"search_query": "abc123"},
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
                "id": str(job.id),
                "data": job_to_json(job),
            }
            for job in jobs
        ],
    }


async def test_search_jobs_by_job_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_search: tuple[dict[str, Job], dict[str, Job], dict[str, Job]],
    mocked_user: MockedUser,
) -> None:
    jobs_by_name, _, jobs_by_address = jobs_search
    # Jobs with ids 0 and 2 has job name `airflow-task` and `airflow-task`
    # Jobs with id 9 has two addresses urls: [http://airflow-host:2080, http://airflow-host:8020]
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
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                "id": str(job.id),
                "data": job_to_json(job),
            }
            for job in jobs
        ],
    }


async def test_search_jobs_by_location_name_and_address_url(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_search: tuple[dict[str, Job], dict[str, Job], dict[str, Job]],
    mocked_user: MockedUser,
) -> None:
    _, jobs_by_location, jobs_by_address = jobs_search
    # Job with id 4 has location name `my-cluster`
    # Job with id 7 has address urls: [`yarn://my_cluster_1`, `yarn://my_cluster_2`]
    jobs = await enrich_jobs(
        [
            jobs_by_location["my-cluster"],
            jobs_by_address["yarn://my_cluster_1"],
        ],
        async_session,
    )

    response = await test_client.get(
        "/v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                "id": str(job.id),
                "data": job_to_json(job),
            }
            for job in jobs
        ],
    }


async def test_search_jobs_no_results(
    test_client: AsyncClient,
    jobs_search: tuple[dict[str, Job], dict[str, Job], dict[str, Job]],
    mocked_user: MockedUser,
) -> None:
    response = await test_client.get(
        "/v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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


async def test_get_job_types(
    test_client: AsyncClient,
    job_types: list[JobType],
    mocked_user: MockedUser,
) -> None:
    unique_job_type = {item.type for item in job_types}
    response = await test_client.get(
        "/v1/jobs/job-types",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {"job_types": sorted(unique_job_type)}


async def test_search_jobs_by_location_id(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_with_locations_and_types: tuple[Job],
    mocked_user: MockedUser,
) -> None:
    jobs = await enrich_jobs(jobs_with_locations_and_types, async_session)

    # first job in jobs has a different location unlike two others
    [_, dag_job, task_job] = jobs
    response = await test_client.get(
        "/v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"location_id": dag_job.location_id},
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
                "id": str(job.id),
                "data": job_to_json(job),
            }
            for job in [dag_job, task_job]
        ],
    }


async def test_search_jobs_by_location_id_non_existen_id(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_with_locations_and_types: tuple[Job],
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "/v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"location_id": -1},
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


async def test_search_by_jobs_type(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_with_locations_and_types: tuple[Job],
    mocked_user: MockedUser,
) -> None:
    jobs = await enrich_jobs(jobs_with_locations_and_types, async_session)
    [_, dag_job, task_job] = jobs
    response = await test_client.get(
        "/v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"job_type": ["AIRFLOW_DAG", "AIRFLOW_TASK"]},
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
                "id": str(job.id),
                "data": job_to_json(job),
            }
            for job in (dag_job, task_job)
        ],
    }


async def test_search_jobs_by_non_existen_type(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_with_locations_and_types: tuple[Job],
    mocked_user: MockedUser,
) -> None:
    response = await test_client.get(
        "/v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"job_type": "NO_EXISTEN_TYPE"},
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
