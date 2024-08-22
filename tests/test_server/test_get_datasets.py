from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from data_rentgen.db.models import Dataset, Location

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_dataset_by_missing_id(
    test_client: AsyncClient,
    new_dataset: Dataset,
):
    response = await test_client.get(
        "v1/datasets",
        params={"dataset_id": new_dataset.id},
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


async def test_get_dataset(
    test_client: AsyncClient,
    dataset: Dataset,
    async_session: AsyncSession,
):
    query = (
        select(Dataset)
        .where(Dataset.id == dataset.id)
        .options(selectinload(Dataset.location).selectinload(Location.addresses))
    )
    dataset_from_db: Dataset = await async_session.scalar(query)

    response = await test_client.get(
        "v1/datasets",
        params={"dataset_id": dataset.id},
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
                "kind": "DATASET",
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


async def test_get_datasets_by_multiple_ids(
    test_client: AsyncClient,
    datasets: list[Dataset],
    async_session: AsyncSession,
):
    # create more objects than pass to endpoint, to test filtering
    dataset_ids = [dataset.id for dataset in datasets[:2]]

    query = (
        select(Dataset)
        .where(Dataset.id.in_(dataset_ids))
        .order_by(Dataset.name)
        .options(selectinload(Dataset.location).selectinload(Location.addresses))
    )
    scalars = await async_session.scalars(query)
    datasets_from_db = list(scalars.all())

    response = await test_client.get(
        "v1/datasets",
        params={"dataset_id": dataset_ids},
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
            for dataset in datasets_from_db
        ],
    }


async def test_get_datasets_no_filters(
    test_client: AsyncClient,
    datasets: list[Dataset],
    async_session: AsyncSession,
):
    query = (
        select(Dataset).order_by(Dataset.name).options(selectinload(Dataset.location).selectinload(Location.addresses))
    )
    scalars = await async_session.scalars(query)
    datasets_from_db = list(scalars.all())

    response = await test_client.get(
        "v1/datasets",
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
            for dataset in datasets_from_db
        ],
    }
