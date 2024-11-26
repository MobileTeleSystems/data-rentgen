from http import HTTPStatus

import pytest
from httpx import AsyncClient

from data_rentgen.db.models import Operation
from tests.fixtures.mocks import MockedUser

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_operations_by_unknown_id(
    test_client: AsyncClient,
    new_operation: Operation,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/operations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"operation_id": str(new_operation.id)},
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


async def test_get_operations_by_one_id(
    test_client: AsyncClient,
    operation: Operation,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/operations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"operation_id": str(operation.id)},
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
                "kind": "OPERATION",
                "id": str(operation.id),
                "created_at": operation.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "run_id": str(operation.run_id),
                "name": operation.name,
                "status": operation.status.name,
                "type": operation.type.value,
                "position": operation.position,
                "group": operation.group,
                "description": operation.description,
                "started_at": operation.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ended_at": operation.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        ],
    }


async def test_get_operations_by_multiple_ids(
    test_client: AsyncClient,
    operations: list[Operation],
    mocked_user: MockedUser,
):
    # create more objects than pass to endpoint, to test filtering
    selected_operations = operations[:2]

    response = await test_client.get(
        "v1/operations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "operation_id": [str(operation.id) for operation in selected_operations],
        },
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
                "kind": "OPERATION",
                "id": str(operation.id),
                "created_at": operation.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "run_id": str(operation.run_id),
                "name": operation.name,
                "status": operation.status.name,
                "type": operation.type.value,
                "position": operation.position,
                "group": operation.group,
                "description": operation.description,
                "started_at": operation.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ended_at": operation.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            for operation in sorted(selected_operations, key=lambda x: (x.run_id.int, -x.id.int))
        ],
    }
