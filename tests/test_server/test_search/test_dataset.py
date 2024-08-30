from http import HTTPStatus

import pytest
from deepdiff import DeepDiff
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from data_rentgen.db.models import Address, Dataset, Location
from tests.test_server.fixtures.factories.dataset import (
    dataset_search_fixture_annotation,
)

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_dataset_search_no_query(test_client: AsyncClient, datasets: list[Dataset]) -> None:

    response = await test_client.get("/v1/datasets/search")

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "detail": [{"input": None, "loc": ["query", "search_query"], "msg": "Field required", "type": "missing"}],
    }


async def test_dataset_search_in_addres_url(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: dataset_search_fixture_annotation,
) -> None:
    _, _, datasets = datasets_search
    # dataset with id 8 has two addresses urls: [hdfs://my-cluster-namenode:2080, hdfs://my-cluster-namenode:8020] and random name
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


async def test_dataset_search_in_location_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: dataset_search_fixture_annotation,
) -> None:

    _, _, datasets = datasets_search
    # Datasets with ids 3 and 4 has location names `postgres.location` and `postgres.history_location`
    # Dataset with id 1 has dataset name `postgres.public.location_history`
    # So this test also cover case: `search in dataset.name + location.name`
    datasets = [datasets[1], datasets[3], datasets[4]]
    query = (
        select(Dataset)
        .where(Dataset.id.in_([dataset.id for dataset in datasets]))
        .options(selectinload(Dataset.location).selectinload(Location.addresses))
    )
    result = await async_session.scalars(query)
    expected_response = {
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
            for dataset in result.all()
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
    response_diff = DeepDiff(expected_response, response.json(), ignore_order=True)
    assert not response_diff, f"Response diff: {response_diff.to_json()}"


async def test_dataset_search_in_dataset_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: dataset_search_fixture_annotation,
) -> None:

    _, _, datasets = datasets_search
    # Dataset with id 1 has dataset name `postgres.public.location_history`
    dataset = datasets[1]
    query = (
        select(Dataset)
        .where(Dataset.id.in_([dataset.id]))
        .options(selectinload(Dataset.location).selectinload(Location.addresses))
    )
    result = await async_session.scalars(query)
    expected_response = {
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
            for dataset in result.all()
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

    response = await test_client.get(
        "/v1/datasets/search",
        params={"search_query": "location_history"},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == expected_response


async def test_dataset_search_in_location_name_and_address_url(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: dataset_search_fixture_annotation,
) -> None:
    _, _, datasets = datasets_search
    # Dataset with id 5 has location name `my-cluster`
    # Dataset with id 8 has address url `hdfs://my-cluster-namenode:2080` and `hdfs://my-cluster-namenode:8020`
    datasets = [datasets[5], datasets[8]]
    query = (
        select(Dataset)
        .where(Dataset.id.in_([dataset.id for dataset in datasets]))
        .options(selectinload(Dataset.location).selectinload(Location.addresses))
    )
    result = await async_session.scalars(query)
    expected_response = {
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
            for dataset in result.all()
        ],
        "meta": {
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "pages_count": 1,
            "previous_page": None,
            "total_count": 2,
        },
    }

    response = await test_client.get(
        "/v1/datasets/search",
        params={"search_query": "my-cluster"},
    )

    assert response.status_code == HTTPStatus.OK

    # At this case the order is unstable
    response_diff = DeepDiff(expected_response, response.json(), ignore_order=True)
    assert not response_diff, f"Response diff: {response_diff.to_json()}"
