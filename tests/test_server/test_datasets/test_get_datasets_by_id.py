from collections.abc import Callable
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Dataset
from data_rentgen.db.models.tag_value import TagValue
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import dataset_to_json, tag_values_to_json
from tests.test_server.utils.enrich import enrich_datasets

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_datasets_by_unknown_id(
    test_client: AsyncClient,
    new_dataset: Dataset,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/datasets",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"dataset_id": new_dataset.id},
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


async def test_get_datasets_by_one_id(
    test_client: AsyncClient,
    dataset: Dataset,
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    datasets = await enrich_datasets([dataset], async_session)
    dataset = datasets[0]

    response = await test_client.get(
        "v1/datasets",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"dataset_id": dataset.id},
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
                "id": str(dataset.id),
                "data": dataset_to_json(dataset),
                "tags": tag_values_to_json(dataset.tags) if dataset.tags else [],
            },
        ],
    }


async def test_get_datasets_by_multiple_ids(
    test_client: AsyncClient,
    datasets: list[Dataset],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    # create more objects than pass to endpoint, to test filtering
    selected_datasets = await enrich_datasets(datasets[:2], async_session)

    response = await test_client.get(
        "v1/datasets",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"dataset_id": [dataset.id for dataset in selected_datasets]},
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
                "id": str(dataset.id),
                "data": dataset_to_json(dataset),
                "tags": tag_values_to_json(dataset.tags) if dataset.tags else [],
            }
            for dataset in sorted(selected_datasets, key=lambda x: x.name)
        ],
    }


async def test_get_datasets_by_one_id_with_tags(
    test_client: AsyncClient,
    dataset_with_tags: Dataset,
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    datasets = await enrich_datasets([dataset_with_tags], async_session)
    dataset = datasets[0]

    response = await test_client.get(
        "v1/datasets",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"dataset_id": dataset.id},
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
                "id": str(dataset.id),
                "data": dataset_to_json(dataset),
                "tags": tag_values_to_json(dataset.tags) if dataset.tags else [],
            },
        ],
    }


async def test_get_datasets_by_tag_value_id(
    test_client: AsyncClient,
    datasets: list[Dataset],
    dataset_with_tags: list[Dataset],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    datasets_with_tags = await enrich_datasets([dataset_with_tags], async_session)
    dataset = datasets_with_tags[0]
    tag_value_id = next(iter(dataset.tags)).id

    response = await test_client.get(
        "v1/datasets",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"tag_value_id": tag_value_id},
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
                "id": str(dataset.id),
                "data": dataset_to_json(dataset),
                "tags": tag_values_to_json(dataset.tags) if dataset.tags else [],
            },
        ],
    }


@pytest.mark.asyncio
async def test_get_datasets_by_multiple_tag_value_ids(
    test_client: AsyncClient,
    tag_values: list[TagValue],
    make_dataset: Callable,
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    tv1, tv2, tv3 = tag_values
    wanted_dataset = await make_dataset(tag_values=tag_values)
    wanted_dataset = (await enrich_datasets([wanted_dataset], async_session))[0]
    await make_dataset(tag_values=[tv1])
    await make_dataset()

    response = await test_client.get(
        "v1/datasets",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params=[("tag_value_id", tv1.id), ("tag_value_id", tv2.id)],
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
                "id": str(wanted_dataset.id),
                "data": dataset_to_json(wanted_dataset),
                "tags": tag_values_to_json(wanted_dataset.tags),
            },
        ],
    }
