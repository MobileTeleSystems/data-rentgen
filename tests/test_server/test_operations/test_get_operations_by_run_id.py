from datetime import timedelta
from http import HTTPStatus

import pytest
from httpx import AsyncClient

from data_rentgen.db.models import Operation, Run
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import operation_to_json

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_operations_missing_since(
    test_client: AsyncClient,
    new_operation: Operation,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/operations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "run_id": str(new_operation.run_id),
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": [],
                    "code": "value_error",
                    "message": "Value error, 'run_id' can be passed only with 'since'",
                    "context": {},
                    "input": {
                        "page": 1,
                        "page_size": 20,
                        "run_id": str(new_operation.run_id),
                        "since": None,
                        "until": None,
                        "operation_id": [],
                    },
                },
            ],
        },
    }


async def test_get_operations_by_unknown_run_id(
    test_client: AsyncClient,
    new_operation: Operation,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/operations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": new_operation.created_at.isoformat(),
            "run_id": str(new_operation.run_id),
        },
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


async def test_get_operations_by_run_id(
    test_client: AsyncClient,
    runs: list[Run],
    operations: list[Operation],
    mocked_user: MockedUser,
):
    run_ids = {operation.run_id for operation in operations}
    runs = [run for run in runs if run.id in run_ids]
    selected_run = runs[0]

    selected_operations = [operation for operation in operations if operation.run_id == selected_run.id]

    since = min(operation.created_at for operation in selected_operations)
    response = await test_client.get(
        "v1/operations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "run_id": str(selected_run.id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(selected_operations),
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [
            {
                "id": str(operation.id),
                "data": operation_to_json(operation),
                "statistics": {
                    "inputs": {
                        "total_datasets": 0,
                        "total_bytes": 0,
                        "total_rows": 0,
                        "total_files": 0,
                    },
                    "outputs": {
                        "total_datasets": 0,
                        "total_bytes": 0,
                        "total_rows": 0,
                        "total_files": 0,
                    },
                },
            }
            for operation in sorted(selected_operations, key=lambda x: (x.created_at, x.id), reverse=True)
        ],
    }


async def test_get_operations_by_run_id_with_until(
    test_client: AsyncClient,
    operations_with_same_run: list[Operation],
    mocked_user: MockedUser,
):
    since = operations_with_same_run[0].created_at
    until = since + timedelta(seconds=1)

    selected_operations = [
        operation for operation in operations_with_same_run if since <= operation.created_at <= until
    ]

    response = await test_client.get(
        "v1/operations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "run_id": str(operations_with_same_run[0].run_id),
            "since": since.isoformat(),
            "until": until.isoformat(),
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
                "id": str(operation.id),
                "data": operation_to_json(operation),
                "statistics": {
                    "inputs": {
                        "total_datasets": 0,
                        "total_bytes": 0,
                        "total_rows": 0,
                        "total_files": 0,
                    },
                    "outputs": {
                        "total_datasets": 0,
                        "total_bytes": 0,
                        "total_rows": 0,
                        "total_files": 0,
                    },
                },
            }
            for operation in sorted(selected_operations, key=lambda x: (x.created_at, x.id), reverse=True)
        ],
    }
