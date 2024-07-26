from datetime import timedelta
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Operation

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_operations_by_run_id_empty(test_client: AsyncClient):
    response = await test_client.get("v1/operations/by_run_id")

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY


async def test_get_operations_by_run_id_missing(
    test_client: AsyncClient,
    new_operation: Operation,
):
    response = await test_client.get(
        "v1/operations/by_run_id",
        params={"since": new_operation.created_at.isoformat(), "run_id": new_operation.run_id},
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


async def test_get_operation_by_run_id(
    test_client: AsyncClient,
    async_session: AsyncSession,
    operation_with_all_fields: Operation,
):
    query = select(Operation).where(Operation.id == operation_with_all_fields.id)
    operation_from_db = await async_session.scalar(query)
    response = await test_client.get(
        "v1/operations/by_run_id",
        params={"since": operation_with_all_fields.created_at.isoformat(), "run_id": operation_with_all_fields.run_id},
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


async def test_get_operations_by_run_id(
    test_client: AsyncClient,
    async_session: AsyncSession,
    operations_with_same_run: list[Operation],
):
    query = (
        select(Operation)
        .where(Operation.id.in_([operation.id for operation in operations_with_same_run]))
        .order_by(Operation.id)
    )
    scalars = await async_session.scalars(query)
    operations_from_db = list(scalars.all())

    response = await test_client.get(
        "v1/operations/by_run_id",
        params={
            "since": operations_with_same_run[0].created_at.isoformat(),
            "run_id": operations_with_same_run[0].run_id,
        },
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": 5,
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
            for operation in operations_from_db
        ],
    }


async def test_get_operations_time_range(
    test_client: AsyncClient,
    async_session: AsyncSession,
    operations_with_same_run: list[Operation],
):
    query = (
        select(Operation)
        .where(Operation.id.in_([operation.id for operation in operations_with_same_run]))
        .order_by(Operation.id)
    )
    scalars = await async_session.scalars(query)
    operations_from_db = list(scalars.all())

    response = await test_client.get(
        "v1/operations/by_run_id",
        params={
            "run_id": operations_with_same_run[0].run_id,
            "since": operations_with_same_run[0].created_at.isoformat(),
            "until": (operations_with_same_run[0].created_at + timedelta(seconds=1)).isoformat(),
        },
    )

    assert response.status_code == HTTPStatus.OK
    assert len(operations_from_db) == 5
    assert response.json()["meta"]["total_count"] == 2
