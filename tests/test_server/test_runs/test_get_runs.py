from datetime import UTC, datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from uuid6 import uuid7

from data_rentgen.db.models.run import Run
from data_rentgen.utils.uuid import generate_new_uuid
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import run_to_json
from tests.test_server.utils.enrich import enrich_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_runs_missing_fields(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": ["query"],
                    "code": "value_error",
                    "message": "Value error, input should contain either 'since', 'run_id', 'job_id', 'parent_run_id' or 'search_query' field",
                    "context": {},
                    "input": {
                        "status": [],
                        "job_type": [],
                        "page": 1,
                        "page_size": 20,
                        "run_id": [],
                    },
                },
            ],
        },
    }


async def test_get_runs_by_run_id_until_less_than_since(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    since = datetime.now(tz=timezone.utc)
    until = since - timedelta(days=1)
    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "run_id": str(uuid7()),
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": ["query", "until"],
                    "code": "value_error",
                    "message": "Value error, 'since' should be less than 'until'",
                    "context": {},
                    "input": until.isoformat(),
                },
            ],
        },
    }


async def test_get_runs_with_since(
    test_client: AsyncClient,
    runs: list[Run],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    since = min(run.created_at for run in runs)
    runs = await enrich_runs(runs, async_session)

    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(runs),
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
            for run in sorted(runs, key=lambda x: (x.created_at, x.id), reverse=True)
        ],
    }


async def test_get_runs_with_until(
    test_client: AsyncClient,
    runs: list[Run],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    since = min(run.created_at for run in runs)
    until = since + timedelta(seconds=1)

    selected_runs = [run for run in runs if since <= run.created_at <= until]
    selected_runs = await enrich_runs(selected_runs, async_session)

    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
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
            for run in sorted(selected_runs, key=lambda x: (x.created_at, x.id), reverse=True)
        ],
    }


async def test_get_runs_unauthorized(
    test_client: AsyncClient,
):
    response = await test_client.get("v1/runs", params={"run_id": str(generate_new_uuid())})

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Missing Authorization header",
            "details": None,
        },
    }


async def test_get_runs_via_personal_token_is_allowed(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.personal_token}"},
        params={
            "since": datetime.now(tz=UTC).isoformat(),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
