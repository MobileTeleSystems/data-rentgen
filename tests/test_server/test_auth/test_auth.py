from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Dataset, Location
from data_rentgen.db.utils.uuid import generate_new_uuid
from tests.fixtures.mocks import MockedUser

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


@pytest.mark.parametrize(
    "endpoint,params",
    [
        ("v1/datasets", {}),
        ("v1/datasets/lineage", {}),
        ("v1/jobs", {}),
        ("v1/jobs/lineage", {}),
        ("v1/operations", {"operation_id": str(generate_new_uuid())}),
        ("v1/operations/lineage", {}),
        ("v1/runs", {"run_id": str(generate_new_uuid())}),
        ("v1/runs/lineage", {}),
        ("v1/locations", {}),
    ],
)
async def test_get_endpoints_without_auth(
    endpoint: str,
    params: dict,
    test_client: AsyncClient,
):
    response = await test_client.get(endpoint, params=params)

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {"code": "unauthorized", "details": None, "message": "Missing auth credentials"},
    }, response.json()


async def test_patch_location_without_auth(
    test_client: AsyncClient,
    async_session: AsyncSession,
    location: Location,
):
    response = await test_client.patch(
        f"v1/locations/{location.id}",
        json={"external_id": "external_id"},
    )

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {"code": "unauthorized", "details": None, "message": "Missing auth credentials"},
    }, response.json()


async def tests_invalid_token(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/datasets",
        headers={"Authorization": f"Bearer {mocked_user.access_token + 'invalid'}"},
    )

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {"code": "unauthorized", "details": None, "message": "Invalid token"},
    }, response.json()
