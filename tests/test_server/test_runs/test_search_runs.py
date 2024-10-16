from http import HTTPStatus

import pytest
from deepdiff import DeepDiff
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Run
from tests.test_server.utils.enrich import enrich_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_search_runs_by_external_id(
    test_client: AsyncClient,
    async_session: AsyncSession,
    runs_search: dict[str, Run],
) -> None:
    runs = await enrich_runs(
        [runs_search["application_1638922609021_0001"], runs_search["application_1638922609021_0002"]],
        async_session,
    )

    response = await test_client.get(
        "/v1/runs",
        params={"search_query": "1638922609021"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()

    expected_response = {
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

    # At this case the order is unstable
    response_diff = DeepDiff(expected_response, response.json(), ignore_order=True)
    assert not response_diff, f"Response diff: {response_diff.to_json()}"


async def test_search_runs_by_job_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    runs_search: dict[str, Run],
) -> None:
    runs = await enrich_runs([runs_search["extract_task_0001"], runs_search["extract_task_0002"]], async_session)

    response = await test_client.get(
        "/v1/runs",
        params={"search_query": "airflow_dag"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()

    expected_response = {
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

    # At this case the order is unstable
    response_diff = DeepDiff(expected_response, response.json(), ignore_order=True)
    assert not response_diff, f"Response diff: {response_diff.to_json()}"


async def test_search_runs_by_job_type(
    test_client: AsyncClient,
    async_session: AsyncSession,
    runs_search: dict[str, Run],
) -> None:
    runs = await enrich_runs(
        [runs_search["application_1638922609021_0001"], runs_search["application_1638922609021_0002"]],
        async_session,
    )

    response = await test_client.get(
        "/v1/runs",
        params={"search_query": "SPARK"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()

    expected_response = {
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

    # At this case the order is unstable
    response_diff = DeepDiff(expected_response, response.json(), ignore_order=True)
    assert not response_diff, f"Response diff: {response_diff.to_json()}"
