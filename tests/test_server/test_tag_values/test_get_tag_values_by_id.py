from http import HTTPStatus

import pytest
from httpx import AsyncClient

from data_rentgen.db.models import TagValue
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import tag_value_to_json

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_tag_values_by_unknown_tag_value_id(
    test_client: AsyncClient,
    new_tag_value: TagValue,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/tag-values",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"tag_value_id": new_tag_value.id},
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


async def test_get_tag_values_by_one_tag_id(
    test_client: AsyncClient,
    tag_values: list[TagValue],
    mocked_user: MockedUser,
):
    tag_value = tag_values[0]
    response = await test_client.get(
        "v1/tag-values",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"tag_value_id": tag_value.id},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
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
                "id": tag_value.id,
                "data": tag_value_to_json(tag_value),
            },
        ],
    }


async def test_get_tag_values_by_multiple_ids(
    test_client: AsyncClient,
    tag_values: list[TagValue],
    mocked_user: MockedUser,
):
    selected_tag_values = tag_values[:2]
    response = await test_client.get(
        "v1/tag-values",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"tag_value_id": [tag_value.id for tag_value in selected_tag_values]},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(selected_tag_values),
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [
            {
                "id": tag_value.id,
                "data": tag_value_to_json(tag_value),
            }
            for tag_value in sorted(selected_tag_values, key=lambda tag: tag.value)
        ],
    }
