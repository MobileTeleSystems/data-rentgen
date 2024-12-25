from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from uuid6 import uuid7

from data_rentgen.db.utils.uuid import generate_new_uuid
from tests.fixtures.mocks import MockedUser

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_operations_missing_fields(test_client: AsyncClient, mocked_user: MockedUser):
    response = await test_client.get(
        "v1/operations",
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
                    "message": "Value error, input should contain either 'run_id' or 'operation_id' field",
                    "context": {},
                    "input": {
                        "page": 1,
                        "page_size": 20,
                        "since": None,
                        "until": None,
                        "operation_id": [],
                        "run_id": None,
                    },
                },
            ],
        },
    }


async def test_get_operations_until_less_than_since(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    since = datetime.now(tz=timezone.utc)
    until = since - timedelta(days=1)
    response = await test_client.get(
        "v1/operations",
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


async def test_get_operations_unauthorized(
    test_client: AsyncClient,
):
    response = await test_client.get("v1/operations", params={"operation_id": str(generate_new_uuid())})

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {"code": "unauthorized", "details": None, "message": "Missing auth credentials"},
    }, response.json()
