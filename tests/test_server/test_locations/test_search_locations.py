from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Location
from tests.test_server.utils.enrich import enrich_locations

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_search_locations_by_address_url(
    test_client: AsyncClient,
    async_session: AsyncSession,
    locations_search: tuple[dict[str, Location], dict[str, Location]],
) -> None:
    _, locations_by_address = locations_search
    locations = await enrich_locations([locations_by_address["hdfs://my-cluster-namenode:8020"]], async_session)

    response = await test_client.get(
        "/v1/locations",
        # search by word prefix
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
            "total_count": 1,
        },
        "items": [
            {
                "id": location.id,
                "name": location.name,
                "type": location.type,
                "addresses": [{"url": address.url} for address in location.addresses],
                "external_id": location.external_id,
            }
            for location in locations
        ],
    }


async def test_search_locations_by_location_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    locations_search: tuple[dict[str, Location], dict[str, Location]],
) -> None:
    locations_by_name, _ = locations_search
    locations = await enrich_locations(
        [locations_by_name["postgres.public.users"]],
        async_session,
    )

    response = await test_client.get(
        "/v1/locations",
        params={"search_query": "postgres.public"},
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
                "id": location.id,
                "name": location.name,
                "type": location.type,
                "addresses": [{"url": address.url} for address in location.addresses],
                "external_id": location.external_id,
            }
            for location in locations
        ],
    }


async def test_search_locations_by_location_name_and_address_url(
    test_client: AsyncClient,
    async_session: AsyncSession,
    locations_search: tuple[dict[str, Location], dict[str, Location]],
) -> None:
    locations_by_location, locations_by_address = locations_search
    # Location name `my-cluster` and address url `hdfs://my-cluster-namenode:8020`
    # Location name `warehouse` and address url `hdfs://warehouse-cluster-namenode:2080`
    locations = await enrich_locations(
        [
            locations_by_location["my-cluster"],
            locations_by_address["hdfs://warehouse-cluster-namenode:2080"],
        ],
        async_session,
    )

    response = await test_client.get(
        "/v1/locations",
        params={"search_query": "cluster"},
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
                "id": location.id,
                "name": location.name,
                "type": location.type,
                "addresses": [{"url": address.url} for address in location.addresses],
                "external_id": location.external_id,
            }
            for location in locations
        ],
    }


async def test_search_locations_no_results(
    test_client: AsyncClient,
    locations_search: tuple[dict[str, Location], dict[str, Location]],
) -> None:
    response = await test_client.get(
        "/v1/locations",
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
