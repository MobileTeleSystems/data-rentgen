from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Dataset
from tests.test_server.utils.enrich import enrich_datasets

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_datasets_by_unknown_id(
    test_client: AsyncClient,
    new_dataset: Dataset,
):
    response = await test_client.get(
        "v1/datasets",
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
):
    datasets = await enrich_datasets([dataset], async_session)
    dataset = datasets[0]

    response = await test_client.get(
        "v1/datasets",
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
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                },
            },
        ],
    }


async def test_get_datasets_by_multiple_ids(
    test_client: AsyncClient,
    datasets: list[Dataset],
    async_session: AsyncSession,
):
    # create more objects than pass to endpoint, to test filtering
    selected_datasets = await enrich_datasets(datasets[:2], async_session)

    response = await test_client.get(
        "v1/datasets",
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
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                },
            }
            for dataset in sorted(selected_datasets, key=lambda x: x.name)
        ],
    }
