from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from data_rentgen.db.models import Dataset, Location

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_dataset_empty(test_client: AsyncClient):
    response = await test_client.get(
        "v1/dataset",
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY


async def test_get_dataset_missing(
    test_client: AsyncClient,
    new_dataset: Dataset,
):
    response = await test_client.get(
        f"v1/dataset?dataset_id={new_dataset.id}",
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


async def test_get_dataset(async_session: AsyncSession, test_client: AsyncClient, dataset: Dataset):
    query = (
        select(Dataset)
        .where(Dataset.id == dataset.id)
        .options(selectinload(Dataset.location).selectinload(Location.addresses))
    )
    dataset_from_db: Dataset = await async_session.scalar(query)

    response = await test_client.get(
        f"v1/dataset?dataset_id={dataset.id}",
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
                "id": dataset_from_db.id,
                "format": dataset_from_db.format,
                "name": dataset_from_db.name,
                "location": {
                    "name": dataset_from_db.location.name,
                    "type": dataset_from_db.location.type,
                    "addresses": [
                        {"url": dataset_from_db.location.addresses[0].url},
                    ],
                },
            },
        ],
    }


async def test_get_datasets(async_session: AsyncSession, test_client: AsyncClient, datasets: list[Dataset]):
    query = (
        select(Dataset)
        .where(Dataset.id.in_([dataset.id for dataset in datasets]))
        .order_by(Dataset.id)
        .options(selectinload(Dataset.location).selectinload(Location.addresses))
    )
    scalars = await async_session.scalars(query)
    datasets_from_db = list(scalars.all())

    response = await test_client.get(
        f"v1/dataset?dataset_id={datasets[0].id}&dataset_id={datasets[1].id}",
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
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                },
            }
            for dataset in datasets_from_db[:2]
        ],
    }
