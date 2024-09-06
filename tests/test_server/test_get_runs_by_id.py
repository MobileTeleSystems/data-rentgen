from http import HTTPStatus
from uuid import uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select
from uuid6 import uuid7

from data_rentgen.db.models import Run
from tests.test_server.utils.enrich import enrich_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_runs_by_id_missing_fields(
    test_client: AsyncClient,
):
    response = await test_client.get("v1/runs")

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": [],
                    "code": "value_error",
                    "message": "Value error, input should contain either 'job_id' and 'since', or 'run_id' field",
                    "context": {},
                    "input": {
                        "page": 1,
                        "page_size": 20,
                        "since": None,
                        "run_id": [],
                        "job_id": None,
                        "until": None,
                    },
                },
            ],
        },
    }


@pytest.mark.parametrize(
    "run_ids",
    [(uuid4()), (uuid4(), uuid7())],
)
async def test_wrong_uuid_version(
    test_client: AsyncClient,
    run_ids,
):
    response = await test_client.get(
        "v1/runs",
        params={"run_id": run_ids},
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY


async def test_get_runs_by_missing_id(
    test_client: AsyncClient,
    new_run: Run,
):
    response = await test_client.get(
        "v1/runs",
        params={"run_id": str(new_run.id)},
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


async def test_get_runs_by_one_id(
    test_client: AsyncClient,
    run: Run,
    async_session: AsyncSession,
):
    runs = await enrich_runs([run], async_session)
    run = runs[0]

    response = await test_client.get(
        "v1/runs",
        params={"run_id": str(run.id)},
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
            },
        ],
    }


async def test_get_runs_by_multiple_ids(
    test_client: AsyncClient,
    runs: list[Run],
    async_session: AsyncSession,
):
    # create more objects than pass to endpoint, to test filtering
    selected_runs = await enrich_runs(runs[:2], async_session)

    response = await test_client.get(
        "v1/runs",
        params={"run_id": [str(run.id) for run in selected_runs]},
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
