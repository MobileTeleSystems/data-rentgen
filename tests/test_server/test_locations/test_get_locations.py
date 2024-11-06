from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Location
from tests.test_server.utils.enrich import enrich_locations

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_locations_no_filters(
    test_client: AsyncClient,
    locations: list[Location],
    async_session: AsyncSession,
):
    locations = await enrich_locations(locations, async_session)
    response = await test_client.get("v1/locations")

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(locations),
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
            for location in sorted(locations, key=lambda x: x.name)
        ],
    }


async def test_get_locations_with_type_filter(
    test_client: AsyncClient,
    locations: list[Location],
    async_session: AsyncSession,
):
    location_type = locations[0].type
    locations = [location for location in locations if location.type == location_type]
    locations = await enrich_locations(locations, async_session)

    response = await test_client.get("v1/locations", params={"location_type": location_type})

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(locations),
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
            for location in sorted(locations, key=lambda x: x.name)
        ],
    }
