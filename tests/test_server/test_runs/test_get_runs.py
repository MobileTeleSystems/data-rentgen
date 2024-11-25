from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from uuid6 import uuid7

from tests.fixtures.mocks import MockedUser

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
                    "location": [],
                    "code": "value_error",
                    "message": "Value error, input should contain either 'run_id', 'job_id', 'parent_run_id' or 'search_query' field",
                    "context": {},
                    "input": {
                        "page": 1,
                        "page_size": 20,
                        "since": None,
                        "until": None,
                        "run_id": [],
                        "parent_run_id": None,
                        "job_id": None,
                        "search_query": None,
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
                    "location": ["until"],
                    "code": "value_error",
                    "message": "Value error, 'since' should be less than 'until'",
                    "context": {},
                    "input": until.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                },
            ],
        },
    }
