from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from uuid6 import uuid7

from data_rentgen.db.models import Run
from tests.test_server.utils.enrich import enrich_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_runs_by_parent_run_id_missing_since(
    test_client: AsyncClient,
):
    parent_run_id = str(uuid7())
    response = await test_client.get(
        "v1/runs",
        params={"parent_run_id": parent_run_id},
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
                    "message": "Value error, input should contain 'since' field if 'parent_run_id' is set",
                    "context": {},
                    "input": {
                        "page": 1,
                        "page_size": 20,
                        "parent_run_id": parent_run_id,
                        "run_id": [],
                        "job_id": None,
                        "since": None,
                        "until": None,
                    },
                },
            ],
        },
    }


async def test_get_runs_by_parent_run_id_conflicting_fields(
    test_client: AsyncClient,
    new_run: Run,
):
    since = datetime.now(tz=timezone.utc)
    response = await test_client.get(
        "v1/runs",
        params={
            "since": since.isoformat(),
            "parent_run_id": str(new_run.parent_run_id),
            "job_id": new_run.job_id,
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
                    "message": "Value error, fields 'job_id' and 'run_id' cannot be used if 'parent_run_id' is set",
                    "context": {},
                    "input": {
                        "page": 1,
                        "page_size": 20,
                        "parent_run_id": str(new_run.parent_run_id),
                        "run_id": [],
                        "job_id": new_run.job_id,
                        "since": since.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "until": None,
                    },
                },
            ],
        },
    }


async def test_get_runs_by_parent_run_id_missing(
    test_client: AsyncClient,
    new_run: Run,
) -> None:
    since = new_run.created_at

    response = await test_client.get(
        "v1/runs",
        params={
            "since": since.isoformat(),
            "parent_run_id": str(new_run.parent_run_id),
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


async def test_get_runs_by_parent_run_id(
    test_client: AsyncClient,
    async_session: AsyncSession,
    runs_with_same_parent: list[Run],
) -> None:
    since = min(run.created_at for run in runs_with_same_parent)
    runs = await enrich_runs(runs_with_same_parent, async_session)

    response = await test_client.get(
        "v1/runs",
        params={
            "since": since.isoformat(),
            "parent_run_id": str(runs_with_same_parent[0].parent_run_id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": 5,
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
                "start_reason": run.start_reason.value,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run.end_reason,
            }
            for run in sorted(runs, key=lambda x: x.id)
        ],
    }


async def test_get_runs_by_parent_run_id_with_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    runs_with_same_parent: list[Run],
) -> None:
    since = min(run.created_at for run in runs_with_same_parent)
    until = since + timedelta(seconds=1)

    selected_runs = [run for run in runs_with_same_parent if since <= run.created_at <= until]
    runs = await enrich_runs(selected_runs, async_session)

    response = await test_client.get(
        "v1/runs",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "parent_run_id": str(runs_with_same_parent[0].parent_run_id),
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
                "start_reason": run.start_reason.value,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run.end_reason,
            }
            for run in sorted(runs, key=lambda x: x.id)
        ],
    }
