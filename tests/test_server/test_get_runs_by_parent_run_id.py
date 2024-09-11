from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select
from uuid6 import uuid7

from data_rentgen.db.models import Run
from tests.test_server.utils.enrich import enrich_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


@pytest.mark.parametrize(
    "field,value",
    [
        ("since", datetime.now(tz=timezone.utc)),
        ("until", datetime.now(tz=timezone.utc)),
    ],
)
async def test_get_runs_by_parent_run_id_missing_fields(
    test_client: AsyncClient,
    field: str,
    value: datetime,
) -> None:
    expected_response = {
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
                        "since": None,
                        "run_id": [],
                        "job_id": None,
                        "until": None,
                    },
                },
            ],
        },
    }
    expected_response["error"]["details"][0]["input"][field] = value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    response = await test_client.get(
        "v1/runs",
        params={field: value.isoformat()},
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
    assert response.json() == expected_response


async def test_get_runs_by_parent_run_id_parent_run_id_without_since_and_until(
    test_client: AsyncClient,
) -> None:
    run_id = str(uuid7())
    expected_response = {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": [],
                    "code": "value_error",
                    "message": "Value error, input should contain 'since' and 'until' fields if 'parent_run_id' is set",
                    "context": {},
                    "input": {
                        "page": 1,
                        "page_size": 20,
                        "parent_run_id": run_id,
                        "since": None,
                        "run_id": [],
                        "job_id": None,
                        "until": None,
                    },
                },
            ],
        },
    }

    response = await test_client.get(
        "v1/runs",
        params={"parent_run_id": run_id},
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
    assert response.json() == expected_response


async def test_get_runs_by_parent_run_id_missing(
    test_client: AsyncClient,
    new_run: Run,
) -> None:
    since = new_run.created_at
    until = since + timedelta(days=1)

    response = await test_client.get(
        "v1/runs",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
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
    until = max(run.created_at for run in runs_with_same_parent)
    runs = await enrich_runs(runs_with_same_parent, async_session)

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
