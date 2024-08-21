from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Operation

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_operations_by_run_id_missing_fields(test_client: AsyncClient):
    since = datetime.now(tz=timezone.utc)
    response = await test_client.get(
        "v1/operations",
        params={"since": since.isoformat()},
    )

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
                        "since": since.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "operation_id": [],
                        "run_id": None,
                        "until": None,
                    },
                },
            ],
        },
    }


async def test_get_operations_by_run_id_conflicting_fields(
    test_client: AsyncClient,
    new_operation: Operation,
):
    response = await test_client.get(
        "v1/operations",
        params={
            "since": new_operation.created_at.isoformat(),
            "run_id": str(new_operation.run_id),
            "operation_id": str(new_operation.id),
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": [],
                    "code": "value_error",
                    "message": "Value error, fields 'run_id','since', 'until' cannot be used if 'operation_id' is set",
                    "context": {},
                    "input": {
                        "page": 1,
                        "page_size": 20,
                        "run_id": str(new_operation.run_id),
                        "operation_id": [str(new_operation.id)],
                        "since": new_operation.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "until": None,
                    },
                },
            ],
        },
    }


async def test_get_operations_by_run_id_until_less_than_since(
    test_client: AsyncClient,
    new_operation: Operation,
):
    since = new_operation.created_at
    until = since - timedelta(days=1)
    response = await test_client.get(
        "v1/operations",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "run_id": str(new_operation.run_id),
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": ["until"],
                    "code": "value_error",
                    "message": "Value error, 'since' should be less than 'until'",
                    "context": {},
                    "input": until.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                },
            ],
        },
    }


async def test_get_operations_by_missing_run_id(
    test_client: AsyncClient,
    new_operation: Operation,
):
    response = await test_client.get(
        "v1/operations",
        params={
            "since": new_operation.created_at.isoformat(),
            "run_id": str(new_operation.run_id),
        },
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


async def test_get_operations_by_one_run_id(
    test_client: AsyncClient,
    async_session: AsyncSession,
    operation: Operation,
):
    query = select(Operation).where(Operation.id == operation.id)
    operation_from_db: Operation = await async_session.scalar(query)

    response = await test_client.get(
        "v1/operations",
        params={
            "since": operation.created_at.isoformat(),
            "run_id": str(operation.run_id),
        },
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


async def test_get_operations_by_multiple_run_ids(
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
        "v1/operations",
        params={
            "since": operations_with_same_run[0].created_at.isoformat(),
            "run_id": str(operations_with_same_run[0].run_id),
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
                "position": operation.position,
                "description": operation.description,
                "started_at": operation.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ended_at": operation.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            for operation in operations_from_db
        ],
    }


async def test_get_operations_by_run_id_with_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    operations_with_same_run: list[Operation],
):
    since = operations_with_same_run[0].created_at
    until = since + timedelta(seconds=1)

    query = (
        select(Operation)
        .where(Operation.id.in_([operation.id for operation in operations_with_same_run]))
        .where(Operation.created_at >= since)
        .where(Operation.created_at <= until)
        .order_by(Operation.id)
    )
    scalars = await async_session.scalars(query)
    operations_from_db = list(scalars.all())

    response = await test_client.get(
        "v1/operations",
        params={
            "run_id": str(operations_with_same_run[0].run_id),
            "since": since.isoformat(),
            "until": until.isoformat(),
        },
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
