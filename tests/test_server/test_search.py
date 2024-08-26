from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from data_rentgen.db.models import Dataset, Location

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_search_no_query(test_client: AsyncClient, datasets: list[Dataset]) -> None:

    response = await test_client.get("/v1/datasets/search")

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "detail": [{"input": None, "loc": ["query", "search_query"], "msg": "Field required", "type": "missing"}],
    }


@pytest.mark.parametrize("search_query", ["history", "postgres/public"])
async def test_search_by_name(
    search_query: str,
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_with_clear_name: list[Dataset],
) -> None:
    datasets = datasets_with_clear_name
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
        "/v1/datasets/search",
        params={"search_query": search_query},
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
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                },
            }
            for dataset in datasets_from_db
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
