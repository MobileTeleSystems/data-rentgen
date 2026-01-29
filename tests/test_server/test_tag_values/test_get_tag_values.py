from http import HTTPStatus

import pytest
from httpx import AsyncClient

from data_rentgen.db.models.tag_value import TagValue
from tests.fixtures.mocks import MockedUser

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_tag_values_no_filters(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/tag-values",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "code": "value_error",
                    "context": {},
                    "input": {
                        "page": 1,
                        "page_size": 20,
                        "tag_value_id": [],
                    },
                    "location": [
                        "query",
                    ],
                    "message": "Value error, input should contain either 'tag_id' or 'tag_value_id' field",
                },
            ],
        },
    }


async def test_get_tag_values_unauthorized(test_client: AsyncClient):
    response = await test_client.get("v1/tag-values")
    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Missing Authorization header",
            "details": None,
        },
    }


async def test_get_tag_values_via_personal_token_is_allowed(
    test_client: AsyncClient,
    mocked_user: MockedUser,
    tag_values: list[TagValue],
):
    tag_value = tag_values[0]
    response = await test_client.get(
        "v1/tag-values",
        headers={"Authorization": f"Bearer {mocked_user.personal_token}"},
        params={"tag_id": tag_value.tag_id},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
