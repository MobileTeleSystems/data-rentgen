from http import HTTPStatus

import pytest
from httpx import AsyncClient

from data_rentgen.db.models import Tag
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import tag_value_to_json

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_search_tag_values(
    test_client: AsyncClient,
    tags_search: dict[str, Tag],
    mocked_user: MockedUser,
) -> None:
    tag = tags_search["company.product"]
    tag_value = next(item for item in tag.tag_values if item.value == 'ETL (Department "IT.DataOps")')

    response = await test_client.get(
        "/v1/tag-values",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"search_query": "dataops", "tag_id": tag.id},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "pages_count": 1,
            "previous_page": None,
            "total_count": 1,
        },
        "items": [
            {
                "id": tag_value.id,
                "data": tag_value_to_json(tag_value),
            }
        ],
    }


async def test_search_tag_values_no_results(
    test_client: AsyncClient,
    tags_search: dict[str, Tag],
    mocked_user: MockedUser,
) -> None:
    tag = tags_search["company.product"]

    response = await test_client.get(
        "/v1/tag-values",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"search_query": "not-found", "tag_id": tag.id},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "pages_count": 1,
            "previous_page": None,
            "total_count": 0,
        },
        "items": [],
    }
