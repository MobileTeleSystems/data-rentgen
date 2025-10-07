from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Dataset, Location
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import dataset_to_json
from tests.test_server.utils.enrich import enrich_datasets

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_datasets_by_location_id(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: tuple[dict[str, Dataset], ...],
    mocked_user: MockedUser,
) -> None:
    _, _, datasets_by_address = datasets_search
    datasets = await enrich_datasets([datasets_by_address["hdfs://my-cluster-namenode:2080"]], async_session)
    location_id = datasets[0].location_id

    response = await test_client.get(
        "/v1/datasets",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"location_id": location_id},
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
                "id": str(dataset.id),
                "data": dataset_to_json(dataset),
                "tags": [],
            }
            for dataset in datasets
        ],
    }


async def test_get_datasets_by_location_id_non_existent(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: tuple[dict[str, Dataset], ...],
    mocked_user: MockedUser,
) -> None:
    response = await test_client.get(
        "/v1/datasets",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"location_id": -1},
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


async def test_get_datasets_by_location_type(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: tuple[dict[str, Dataset], ...],
    mocked_user: MockedUser,
) -> None:
    # random locations created by datasets_search fixture can also have type=hdfs
    datasets_query = (
        select(Dataset)
        .join(Location, Location.id == Dataset.location_id)
        .where(Location.type == "hdfs")
        .order_by(Dataset.name)
    )

    dataset_scalars = await async_session.scalars(datasets_query)
    async_session.expunge_all()

    datasets = await enrich_datasets(list(dataset_scalars.all()), async_session)

    response = await test_client.get(
        "/v1/datasets",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"location_type": ["HDFS"]},  # case-insensitive
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
            "total_count": len(datasets),
        },
        "items": [
            {
                "id": str(dataset.id),
                "data": dataset_to_json(dataset),
                "tags": [],
            }
            for dataset in datasets
        ],
    }


async def test_get_datasets_by_location_type_non_existent(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: tuple[dict[str, Dataset], ...],
    mocked_user: MockedUser,
) -> None:
    response = await test_client.get(
        "/v1/datasets",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"location_type": "non_existent_location_type"},
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
