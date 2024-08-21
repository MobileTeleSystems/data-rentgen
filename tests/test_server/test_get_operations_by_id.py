from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

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


async def test_get_operations_by_missing_id(
    test_client: AsyncClient,
    new_operation: Operation,
):
    response = await test_client.get(
        "v1/operations",
        params={"operation_id": new_operation.id},
    )

    assert response.status_code == HTTPStatus.OK
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
    async_session: AsyncSession,
):
    query = select(Operation).where(Operation.id == operation.id)
    operation_from_db: Operation = await async_session.scalar(query)

    response = await test_client.get(
        "v1/operations",
        params={"operation_id": operation.id},
    )

    assert response.status_code == HTTPStatus.OK
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
                "id": str(operation_from_db.id),
                "run_id": str(operation_from_db.run_id),
                "name": operation_from_db.name,
                "status": operation_from_db.status.value,
                "type": operation_from_db.type.value,
                "position": operation_from_db.position,
                "description": operation_from_db.description,
                "started_at": operation_from_db.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ended_at": operation_from_db.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        ],
    }


async def test_get_operations_by_multiple_ids(
    test_client: AsyncClient,
    operations: list[Operation],
    async_session: AsyncSession,
):
    # create more objects than pass to endpoint, to test filtering
    operation_ids = [operation.id for operation in operations[:2]]
    query = select(Operation).where(Operation.id.in_(operation_ids)).order_by(Operation.run_id, Operation.id)
    scalars = await async_session.scalars(query)
    operations_from_db = list(scalars.all())

    response = await test_client.get(
        "v1/operations",
        params={"operation_id": [str(id) for id in operation_ids]},
    )

    assert response.status_code == HTTPStatus.OK
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
            for operation in operations_from_db
        ],
    }
