from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Location
from tests.test_server.utils.enrich import enrich_locations

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_locations_by_unknown_id(
    test_client: AsyncClient,
    new_location: Location,
):
    response = await test_client.get(
        "v1/locations",
        params={"location_id": new_location.id},
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


async def test_get_locations_by_one_id(
    test_client: AsyncClient,
    location: Location,
    async_session: AsyncSession,
):
    [location] = await enrich_locations([location], async_session)

    response = await test_client.get(
        "v1/locations",
        params={"location_id": location.id},
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
                "id": location.id,
                "name": location.name,
                "type": location.type,
                "addresses": [{"url": address.url} for address in location.addresses],
                "external_id": location.external_id,
            },
        ],
    }


async def test_get_locations_by_multiple_ids(
    test_client: AsyncClient,
    locations: list[Location],
    async_session: AsyncSession,
):
    # create more objects than pass to endpoint, to test filtering
    selected_locations = await enrich_locations(locations[:2], async_session)

    response = await test_client.get(
        "v1/locations",
        params={"location_id": [location.id for location in selected_locations]},
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
                "id": location.id,
                "name": location.name,
                "type": location.type,
                "addresses": [{"url": address.url} for address in location.addresses],
                "external_id": location.external_id,
            }
            for location in sorted(selected_locations, key=lambda x: x.name)
        ],
    }
