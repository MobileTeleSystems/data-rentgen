from http import HTTPStatus

import pytest
from deepdiff import DeepDiff
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from data_rentgen.db.models import Dataset, Location
from tests.test_server.fixtures.factories.dataset import (
    dataset_search_fixture_annotation,
)

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_search_no_query(test_client: AsyncClient, datasets: list[Dataset]) -> None:

    response = await test_client.get("/v1/datasets/search")

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "detail": [{"input": None, "loc": ["query", "search_query"], "msg": "Field required", "type": "missing"}],
    }


async def test_search_in_addres_url(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: dataset_search_fixture_annotation,
) -> None:
    _, _, datasets = datasets_search
    dataset = datasets[8]
    query = (
        select(Location)
        .join(Dataset, Dataset.location_id == Location.id)
        .options(selectinload(Location.addresses))
        .where(Dataset.id == dataset.id)
    )
    location = await async_session.scalar(query)

    response = await test_client.get(
        "/v1/datasets/search",
        params={"search_query": "namenode"},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "items": [
            {
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "name": location.name,
                    "type": location.type,
                    "addresses": [{"url": address.url} for address in location.addresses],
                },
            },
        ],
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
    }


async def test_search_in_location_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: dataset_search_fixture_annotation,
) -> None:

    _, locations, datasets = datasets_search
    datasets = [datasets[4], datasets[1], datasets[3]]
    query = (
        select(Location)
        .join(Dataset, Dataset.location_id == Location.id)
        .options(selectinload(Location.addresses))
        .where(Dataset.id == datasets[0].id)
    )
    location_0 = await async_session.scalar(query)
    query = (
        select(Location)
        .join(Dataset, Dataset.location_id == Location.id)
        .options(selectinload(Location.addresses))
        .where(Dataset.id == datasets[1].id)
    )
    location_1 = await async_session.scalar(query)
    query = (
        select(Location)
        .join(Dataset, Dataset.location_id == Location.id)
        .options(selectinload(Location.addresses))
        .where(Dataset.id == datasets[2].id)
    )
    location_2 = await async_session.scalar(query)
    locations = [location_0, location_1, location_2]
    expected_response = {
        "items": [
            {
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "name": location.name,
                    "type": location.type,
                    "addresses": [{"url": address.url} for address in location.addresses],
                },
            }
            for dataset, location in zip(datasets, locations)
        ],
        "meta": {
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "pages_count": 1,
            "previous_page": None,
            "total_count": 3,
        },
    }

    response = await test_client.get(
        "/v1/datasets/search",
        params={"search_query": "postgres.location"},
    )

    assert response.status_code == HTTPStatus.OK

    # At this case the order is unstable
    diff = DeepDiff(expected_response, response.json(), ignore_order=True)
    assert diff == {}, response.json()


async def test_search_in_dataset_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: dataset_search_fixture_annotation,
) -> None:

    _, _, datasets = datasets_search
    dataset = datasets[1]
    query = (
        select(Location)
        .join(Dataset, Dataset.location_id == Location.id)
        .options(selectinload(Location.addresses))
        .where(Dataset.id == dataset.id)
    )
    location = await async_session.scalar(query)

    response = await test_client.get(
        "/v1/datasets/search",
        params={"search_query": "location_history"},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "items": [
            {
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "name": location.name,
                    "type": location.type,
                    "addresses": [{"url": address.url} for address in location.addresses],
                },
            },
        ],
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
    }


async def test_search_in_dataset_and_location_names(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: dataset_search_fixture_annotation,
) -> None:
    _, _, datasets = datasets_search
    datasets = [datasets[4], datasets[0], datasets[1]]
    query = (
        select(Location)
        .join(Dataset, Dataset.location_id == Location.id)
        .options(selectinload(Location.addresses))
        .where(Dataset.id == datasets[0].id)
    )
    location_0 = await async_session.scalar(query)
    query = (
        select(Location)
        .join(Dataset, Dataset.location_id == Location.id)
        .options(selectinload(Location.addresses))
        .where(Dataset.id == datasets[1].id)
    )
    location_1 = await async_session.scalar(query)
    query = (
        select(Location)
        .join(Dataset, Dataset.location_id == Location.id)
        .options(selectinload(Location.addresses))
        .where(Dataset.id == datasets[2].id)
    )
    location_2 = await async_session.scalar(query)
    locations = [location_0, location_1, location_2]
    excepted_response = {
        "items": [
            {
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "name": location.name,
                    "type": location.type,
                    "addresses": [{"url": address.url} for address in location.addresses],
                },
            }
            for dataset, location in zip(datasets, locations)
        ],
        "meta": {
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "pages_count": 1,
            "previous_page": None,
            "total_count": 3,
        },
    }

    response = await test_client.get(
        "/v1/datasets/search",
        params={"search_query": "postgres.history"},
    )

    assert response.status_code == HTTPStatus.OK

    diff = DeepDiff(excepted_response, response.json(), ignore_order=True)
    assert diff == {}, diff
