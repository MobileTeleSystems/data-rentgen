from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Job, Run
from tests.test_server.utils.enrich import enrich_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_runs_by_job_id_missing_fields(
    test_client: AsyncClient,
):
    since = datetime.now(tz=timezone.utc)
    response = await test_client.get(
        "v1/runs",
        params={"since": since.isoformat()},
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": [],
                    "code": "value_error",
                    "message": "Value error, input should contain either 'job_id' and 'since' or 'parent_run_id' with 'since' and 'until' or 'run_id'",
                    "context": {},
                    "input": {
                        "page": 1,
                        "page_size": 20,
                        "parent_run_id": None,
                        "since": since.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "run_id": [],
                        "job_id": None,
                        "until": None,
                    },
                },
            ],
        },
    }


async def test_get_runs_by_job_id_conflicting_fields(
    test_client: AsyncClient,
    new_run: Run,
):
    since = datetime.now(tz=timezone.utc)
    response = await test_client.get(
        "v1/runs",
        params={
            "since": since.isoformat(),
            "job_id": new_run.job_id,
            "run_id": str(new_run.id),
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": [],
                    "code": "value_error",
                    "message": "Value error, fields 'job_id','since', 'until', 'parent_run_id' cannot be used if 'run_id' is set",
                    "context": {},
                    "input": {
                        "page": 1,
                        "page_size": 20,
                        "parent_run_id": None,
                        "run_id": [str(new_run.id)],
                        "job_id": new_run.job_id,
                        "since": since.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "until": None,
                    },
                },
            ],
        },
    }


async def test_get_runs_by_job_id_until_less_than_since(
    test_client: AsyncClient,
    new_run: Run,
):
    since = new_run.created_at
    until = since - timedelta(days=1)
    response = await test_client.get(
        "v1/runs",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "job_id": str(new_run.job_id),
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": ["until"],
                    "code": "value_error",
                    "message": "Value error, 'since' should be less than 'until'",
                    "context": {},
                    "input": until.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                },
            ],
        },
    }


async def test_get_runs_by_missing_job_id(
    test_client: AsyncClient,
    new_run: Run,
):
    response = await test_client.get(
        "v1/runs",
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
                "start_reason": run.start_reason,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run.end_reason,
            }
            for run in sorted(selected_runs, key=lambda x: x.id)
        ],
    }


async def test_get_runs_by_job_id_with_until(
    test_client: AsyncClient,
    runs_with_same_job: list[Run],
    async_session: AsyncSession,
):
    since = min(run.created_at for run in runs_with_same_job)
    until = since + timedelta(seconds=1)

    selected_runs = [run for run in runs_with_same_job if since <= run.created_at <= until]
    runs = await enrich_runs(selected_runs, async_session)

    response = await test_client.get(
        "v1/runs",
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
                "start_reason": run.start_reason,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run.end_reason,
            }
            for run in sorted(runs, key=lambda x: x.id)
        ],
    }
