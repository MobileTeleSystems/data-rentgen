from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Operation

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_operations_by_id_empty(test_client: AsyncClient):
    response = await test_client.get("v1/operations/by_id")

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY


async def test_get_operations_by_id_missing(
    test_client: AsyncClient,
    new_operation: Operation,
):
    response = await test_client.get(
        "v1/operations/by_id",
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


async def test_get_operation_by_id(
    test_client: AsyncClient,
    operation_with_all_fields: Operation,
    async_session: AsyncSession,
):
    query = select(Operation).where(Operation.id == operation_with_all_fields.id)
    operation_from_db = await async_session.scalar(query)

    response = await test_client.get(
        "v1/operations/by_id",
        params={"operation_id": operation_with_all_fields.id},
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


async def test_get_operations_by_id(
    test_client: AsyncClient,
    async_session: AsyncSession,
    operations: list[Operation],
):
    query = select(Operation).where(Operation.id.in_([operation.id for operation in operations])).order_by(Operation.id)
    scalars = await async_session.scalars(query)
    operations_from_db = list(scalars.all())

    response = await test_client.get(
        "v1/operations/by_id",
        params={"operation_id": [operation.id for operation in operations[:2]]},
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
                "position": None,
                "description": None,
                "started_at": None,
                "ended_at": None,
            }
            for operation in operations_from_db[:2]
        ],
    }
