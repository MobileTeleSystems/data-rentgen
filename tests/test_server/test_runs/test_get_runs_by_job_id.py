from datetime import timedelta
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Job, Run
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import run_to_json
from tests.test_server.utils.enrich import enrich_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_runs_by_job_id_missing_since(
    test_client: AsyncClient,
    new_run: Run,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "job_id": new_run.job_id,
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
                    "message": "Value error, 'job_id' can be passed only with 'since'",
                    "context": {},
                    "input": {
                        "page": 1,
                        "page_size": 20,
                        "job_id": new_run.job_id,
                        "since": None,
                        "until": None,
                        "parent_run_id": None,
                        "run_id": [],
                        "search_query": None,
                    },
                },
            ],
        },
    }


async def test_get_runs_by_unknown_job_id(
    test_client: AsyncClient,
    new_run: Run,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": new_run.created_at.isoformat(),
            "job_id": new_run.job_id,
        },
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


async def test_get_runs_by_job_id(
    test_client: AsyncClient,
    jobs: list[Job],
    runs: list[Run],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    job_ids = {run.job_id for run in runs}
    jobs = [job for job in jobs if job.id in job_ids]

    selected_job = jobs[0]
    selected_runs = await enrich_runs(
        [run for run in runs if run.job_id == selected_job.id],
        async_session,
    )

    since = min(run.created_at for run in selected_runs)
    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "job_id": selected_job.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(selected_runs),
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [
            {
                "id": str(run.id),
                "data": run_to_json(run),
                "statistics": {
                    "inputs": {
                        "total_datasets": 0,
                        "total_bytes": 0,
                        "total_rows": 0,
                        "total_files": 0,
                    },
                    "outputs": {
                        "total_datasets": 0,
                        "total_bytes": 0,
                        "total_rows": 0,
                        "total_files": 0,
                    },
                    "operations": {
                        "total_operations": 0,
                    },
                },
            }
            for run in sorted(selected_runs, key=lambda x: x.id, reverse=True)
        ],
    }


async def test_get_runs_by_job_id_with_until(
    test_client: AsyncClient,
    runs_with_same_job: list[Run],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    since = min(run.created_at for run in runs_with_same_job)
    until = since + timedelta(seconds=1)

    selected_runs = [run for run in runs_with_same_job if since <= run.created_at <= until]
    runs = await enrich_runs(selected_runs, async_session)

    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "job_id": runs[0].job_id,
            "since": since.isoformat(),
            "until": until.isoformat(),
        },
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
                "id": str(run.id),
                "data": run_to_json(run),
                "statistics": {
                    "inputs": {
                        "total_datasets": 0,
                        "total_bytes": 0,
                        "total_rows": 0,
                        "total_files": 0,
                    },
                    "outputs": {
                        "total_datasets": 0,
                        "total_bytes": 0,
                        "total_rows": 0,
                        "total_files": 0,
                    },
                    "operations": {
                        "total_operations": 0,
                    },
                },
            }
            for run in sorted(runs, key=lambda x: x.id, reverse=True)
        ],
    }
