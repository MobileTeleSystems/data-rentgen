from http import HTTPStatus

import pytest
from httpx import AsyncClient

from data_rentgen.db.models import Operation

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_operations_no_filter(test_client: AsyncClient):
    response = await test_client.get("v1/operations")

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": [],
                    "code": "value_error",
                    "message": "Value error, input should contain either 'run_id' and 'since', or 'operation_id' field",
                    "context": {},
                    "input": {
                        "page": 1,
                        "page_size": 20,
                        "since": None,
                        "operation_id": [],
                        "run_id": None,
                        "until": None,
                    },
                },
            ],
        },
    }


async def test_get_operations_by_unknown_id(
    test_client: AsyncClient,
    new_operation: Operation,
):
    response = await test_client.get(
        "v1/operations",
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
):
    response = await test_client.get(
        "v1/operations",
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
                "run_id": str(operation.run_id),
                "name": operation.name,
                "status": operation.status.value,
                "type": operation.type.value,
                "position": operation.position,
                "description": operation.description,
                "started_at": operation.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ended_at": operation.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        ],
    }


async def test_get_operations_by_multiple_ids(
    test_client: AsyncClient,
    operations: list[Operation],
):
    # create more objects than pass to endpoint, to test filtering
    selected_operations = operations[:2]

    response = await test_client.get(
        "v1/operations",
        params={"operation_id": [str(operation.id) for operation in selected_operations]},
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
                "run_id": str(operation.run_id),
                "name": operation.name,
                "status": operation.status.value,
                "type": operation.type.value,
                "position": operation.position,
                "description": operation.description,
                "started_at": operation.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ended_at": operation.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            for operation in sorted(selected_operations, key=lambda x: (x.run_id.bytes, x.id.bytes))
        ],
    }
