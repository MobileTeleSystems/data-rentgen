from http import HTTPStatus

import pytest
from deepdiff import DeepDiff
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Run
from tests.test_server.utils.enrich import enrich_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_search_runs_missing_since(
    test_client: AsyncClient,
    new_run: Run,
):
    response = await test_client.get(
        "v1/runs",
        params={
            "search_query": new_run.external_id,
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": [],
                    "code": "value_error",
                    "message": "Value error, 'search_query' can be passed only with 'since'",
                    "context": {},
                    "input": {
                        "page": 1,
                        "page_size": 20,
                        "job_id": None,
                        "since": None,
                        "until": None,
                        "parent_run_id": None,
                        "run_id": [],
                        "search_query": new_run.external_id,
                    },
                },
            ],
        },
    }


async def test_search_runs_by_external_id(
    test_client: AsyncClient,
    async_session: AsyncSession,
    runs_search: dict[str, Run],
) -> None:
    runs = await enrich_runs(
        [
            # runs sorted by id in descending order
            runs_search["application_1638922609021_0002"],
            runs_search["application_1638922609021_0001"],
        ],
        async_session,
    )
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "/v1/runs",
        params={
            "since": since.isoformat(),
            # search by word prefix
            "search_query": "1638922",
        },
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
                "kind": "RUN",
                "id": str(run.id),
                "job_id": run.job_id,
                "created_at": run.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "parent_run_id": str(run.parent_run_id),
                "status": run.status.value,
                "external_id": run.external_id,
                "attempt": run.attempt,
                "persistent_log_url": run.persistent_log_url,
                "running_log_url": run.running_log_url,
                "started_at": run.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "started_by_user": {"name": run.started_by_user.name},
                "start_reason": run.start_reason.value,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run.end_reason,
            }
            for run in runs
        ],
    }


async def test_search_runs_by_job_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    runs_search: dict[str, Run],
) -> None:
    runs = await enrich_runs(
        [
            # runs sorted by id in descending order
            runs_search["extract_task_0002"],
            runs_search["extract_task_0001"],
        ],
        async_session,
    )
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "/v1/runs",
        params={
            "since": since.isoformat(),
            "search_query": "airflow_dag",
        },
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
                "kind": "RUN",
                "id": str(run.id),
                "job_id": run.job_id,
                "created_at": run.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "parent_run_id": str(run.parent_run_id),
                "status": run.status.value,
                "external_id": run.external_id,
                "attempt": run.attempt,
                "persistent_log_url": run.persistent_log_url,
                "running_log_url": run.running_log_url,
                "started_at": run.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "started_by_user": {"name": run.started_by_user.name},
                "start_reason": run.start_reason.value,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run.end_reason,
            }
            for run in runs
        ],
    }


async def test_search_runs_by_job_type(
    test_client: AsyncClient,
    async_session: AsyncSession,
    runs_search: dict[str, Run],
) -> None:
    runs = await enrich_runs(
        [
            # runs sorted by id in descending order
            runs_search["application_1638922609021_0002"],
            runs_search["application_1638922609021_0001"],
        ],
        async_session,
    )
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "/v1/runs",
        params={
            "since": since.isoformat(),
            "search_query": "SPARK",
        },
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
                "kind": "RUN",
                "id": str(run.id),
                "job_id": run.job_id,
                "created_at": run.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "parent_run_id": str(run.parent_run_id),
                "status": run.status.value,
                "external_id": run.external_id,
                "attempt": run.attempt,
                "persistent_log_url": run.persistent_log_url,
                "running_log_url": run.running_log_url,
                "started_at": run.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "started_by_user": {"name": run.started_by_user.name},
                "start_reason": run.start_reason.value,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run.end_reason,
            }
            for run in runs
        ],
    }


async def test_search_runs_no_results(
    test_client: AsyncClient,
    runs_search: dict[str, Run],
) -> None:
    since = min(run.created_at for run in runs_search.values())

    response = await test_client.get(
        "/v1/runs",
        params={
            "since": since.isoformat(),
            "search_query": "not-found",
        },
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
