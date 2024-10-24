from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Dataset
from tests.test_server.utils.enrich import enrich_datasets

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_datasets_no_filters(
    test_client: AsyncClient,
    datasets: list[Dataset],
    async_session: AsyncSession,
):
    datasets = await enrich_datasets(datasets, async_session)
    response = await test_client.get("v1/datasets")

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(datasets),
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
                    "external_id": dataset.location.external_id,
                },
            }
            for dataset in sorted(datasets, key=lambda x: x.name)
        ],
    }
