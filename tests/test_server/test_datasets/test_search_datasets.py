from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Dataset
from tests.test_server.utils.enrich import enrich_datasets

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_search_datasets_by_address_url(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: tuple[dict[str, Dataset], dict[str, Dataset], dict[str, Dataset]],
) -> None:
    _, _, datasets_by_address = datasets_search
    # dataset with id 8 has two addresses urls: [hdfs://my-cluster-namenode:2080, hdfs://my-cluster-namenode:8020] and random name
    datasets = await enrich_datasets([datasets_by_address["hdfs://my-cluster-namenode:2080"]], async_session)

    response = await test_client.get(
        "/v1/datasets",
        # search by word prefix
        params={"search_query": "nameno"},
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
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "id": dataset.location.id,
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                    "external_id": dataset.location.external_id,
                },
            }
            for dataset in datasets
        ],
    }


async def test_search_datasets_by_location_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: tuple[dict[str, Dataset], dict[str, Dataset], dict[str, Dataset]],
) -> None:
    datasets_by_name, datasets_by_location, _ = datasets_search
    # Datasets with ids 3 and 4 has location names `postgres.location` and `postgres.history_location`
    # Dataset with id 1 has dataset name `postgres.public.location_history`
    # So this test also cover case: `search in dataset.name + location.name`
    datasets = await enrich_datasets(
        [
            # on top of the search are results with shorter name,
            # then sorted alphabetically
            datasets_by_location["postgres.history_location"],
            datasets_by_location["postgres.location"],
            datasets_by_name["postgres.public.location_history"],
        ],
        async_session,
    )

    response = await test_client.get(
        "/v1/datasets",
        params={"search_query": "postgres.location"},
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
            "total_count": 3,
        },
        "items": [
            {
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "id": dataset.location.id,
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                    "external_id": dataset.location.external_id,
                },
            }
            for dataset in datasets
        ],
    }


async def test_search_datasets_by_dataset_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: tuple[dict[str, Dataset], dict[str, Dataset], dict[str, Dataset]],
) -> None:
    datasets_by_name, _, _ = datasets_search
    # Dataset with id 1 has dataset name `postgres.public.location_history`
    datasets = await enrich_datasets([datasets_by_name["postgres.public.location_history"]], async_session)

    response = await test_client.get(
        "/v1/datasets",
        params={"search_query": "location_history"},
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
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "id": dataset.location.id,
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                    "external_id": dataset.location.external_id,
                },
            }
            for dataset in datasets
        ],
    }


async def test_search_datasets_by_location_name_and_address_url(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: tuple[dict[str, Dataset], dict[str, Dataset], dict[str, Dataset]],
) -> None:
    _, datasets_by_location, datasets_by_address = datasets_search
    # Dataset with id 5 has location name `my-cluster`
    # Dataset with id 8 has address url `hdfs://my-cluster-namenode:2080` and `hdfs://my-cluster-namenode:8020`
    datasets = await enrich_datasets(
        [
            datasets_by_location["my-cluster"],
            datasets_by_address["hdfs://my-cluster-namenode:8020"],
        ],
        async_session,
    )

    response = await test_client.get(
        "/v1/datasets",
        params={"search_query": "my-cluster"},
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
            "total_count": 2,
        },
        "items": [
            {
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "id": dataset.location.id,
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                    "external_id": dataset.location.external_id,
                },
            }
            for dataset in datasets
        ],
    }


async def test_search_datasets_no_results(
    test_client: AsyncClient,
    datasets_search: tuple[dict[str, Dataset], dict[str, Dataset], dict[str, Dataset]],
) -> None:
    response = await test_client.get(
        "/v1/datasets",
        params={"search_query": "not-found"},
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
