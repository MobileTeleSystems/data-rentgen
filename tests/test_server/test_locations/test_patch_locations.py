from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Location
from tests.test_server.utils.enrich import enrich_locations

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


@pytest.mark.parametrize(
    "location",
    [
        pytest.param({"external_id": None}),
    ],
    indirect=True,
)
async def test_add_loction_external_id(
    test_client: AsyncClient,
    async_session: AsyncSession,
    location: Location,
):
    [location] = await enrich_locations([location], async_session)
    assert location.external_id is None

    response = await test_client.patch(
        f"v1/locations/{location.id}",
        json={"external_id": "external_id"},
    )
    [location] = await enrich_locations([location], async_session)

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "name": location.name,
        "type": location.type,
        "addresses": [{"url": address.url} for address in location.addresses],
        "external_id": "external_id",
    }


async def test_update_location_external_id(
    test_client: AsyncClient,
    async_session: AsyncSession,
    location: Location,
):
    response = await test_client.patch(
        f"v1/locations/{location.id}",
        json={"external_id": "new_external_id"},
    )
    [location] = await enrich_locations([location], async_session)

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "name": location.name,
        "type": location.type,
        "addresses": [{"url": address.url} for address in location.addresses],
        "external_id": "new_external_id",
    }


async def test_update_location_not_found(
    test_client: AsyncClient,
    new_location: Location,
):
    response = await test_client.patch(
        f"v1/locations/{new_location.id}",
        json={"external_id": "new_external_id"},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND, response.json()
    assert response.json() == {
        "error": {
            "code": "not_found",
            "details": {
                "entity_type": "Location",
                "field": "id",
                "value": new_location.id,
            },
            "message": f"Location with id={new_location.id} not found",
        },
    }


async def test_update_location_writinf_null_to_external_id(
    test_client: AsyncClient,
    location: Location,
):
    response = await test_client.patch(
        f"v1/locations/{location.id}",
        json={"external_id": None},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "name": location.name,
        "type": location.type,
        "addresses": [{"url": address.url} for address in location.addresses],
        "external_id": None,
    }
