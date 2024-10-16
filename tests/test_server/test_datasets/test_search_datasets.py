from http import HTTPStatus

import pytest
from deepdiff import DeepDiff
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Dataset
from tests.test_server.utils.enrich import enrich_datasets

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_dataset_search_no_query(test_client: AsyncClient) -> None:
    response = await test_client.get("/v1/datasets/search")

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": ["query", "search_query"],
                    "code": "missing",
                    "message": "Field required",
                    "input": None,
                    "context": {},
                },
            ],
        },
    }


async def test_search_datasets_by_address_url(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: dict[str, Dataset],
) -> None:
    # dataset with id 8 has two addresses urls: [hdfs://my-cluster-namenode:2080, hdfs://my-cluster-namenode:8020] and random name
    datasets = await enrich_datasets([datasets_search["hdfs://my-cluster-namenode:2080"]], async_session)

    response = await test_client.get(
        "/v1/datasets/search",
        params={"search_query": "namenode"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    expected_response = {
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
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                },
            }
            for dataset in datasets
        ],
    }
    # At this case the order is unstable
    response_diff = DeepDiff(expected_response, response.json(), ignore_order=True)
    assert not response_diff, f"Response diff: {response_diff.to_json()}"


async def test_search_datasets_by_location_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: dict[str, Dataset],
) -> None:
    # Datasets with ids 3 and 4 has location names `postgres.location` and `postgres.history_location`
    # Dataset with id 1 has dataset name `postgres.public.location_history`
    # So this test also cover case: `search in dataset.name + location.name`
    datasets = await enrich_datasets(
        [
            datasets_search["postgres.public.location_history"],
            datasets_search["postgres.location"],
            datasets_search["postgres.history_location"],
        ],
        async_session,
    )

    response = await test_client.get(
        "/v1/datasets/search",
        params={"search_query": "postgres.location"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()

    expected_response = {
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
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                },
            }
            for dataset in datasets
        ],
    }
    # At this case the order is unstable
    response_diff = DeepDiff(expected_response, response.json(), ignore_order=True)
    assert not response_diff, f"Response diff: {response_diff.to_json()}"


async def test_search_datasets_by_dataset_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: dict[str, Dataset],
) -> None:
    # Dataset with id 1 has dataset name `postgres.public.location_history`
    datasets = await enrich_datasets([datasets_search["postgres.public.location_history"]], async_session)

    response = await test_client.get(
        "/v1/datasets/search",
        params={"search_query": "location_history"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()

    expected_response = {
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
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                },
            }
            for dataset in datasets
        ],
    }

    # At this case the order is unstable
    response_diff = DeepDiff(expected_response, response.json(), ignore_order=True)
    assert not response_diff, f"Response diff: {response_diff.to_json()}"


async def test_search_datasets_by_location_name_and_address_url(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: dict[str, Dataset],
) -> None:
    # Dataset with id 5 has location name `my-cluster`
    # Dataset with id 8 has address url `hdfs://my-cluster-namenode:2080` and `hdfs://my-cluster-namenode:8020`
    datasets = await enrich_datasets(
        [datasets_search["my-cluster"], datasets_search["hdfs://my-cluster-namenode:8020"]],
        async_session,
    )

    response = await test_client.get(
        "/v1/datasets/search",
        params={"search_query": "my-cluster"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()

    expected_response = {
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
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                },
            }
            for dataset in datasets
        ],
    }

    # At this case the order is unstable
    response_diff = DeepDiff(expected_response, response.json(), ignore_order=True)
    assert not response_diff, f"Response diff: {response_diff.to_json()}"
