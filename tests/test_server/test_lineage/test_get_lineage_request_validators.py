from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient

from data_rentgen.db.utils.uuid import generate_new_uuid

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


@pytest.mark.parametrize(
    "entity_kind,granularity",
    [
        ("operations", "OPERATION"),
        ("datasets", "OPERATION"),
        ("runs", "RUN"),
        ("jobs", "OPERATION"),
    ],
)
async def test_get_lineage_no_filter(test_client: AsyncClient, entity_kind: str, granularity: str):
    response = await test_client.get(f"v1/{entity_kind}/lineage")

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "details": [
                {
                    "code": "missing",
                    "context": {},
                    "input": {"depth": 1, "granularity": granularity},
                    "location": ["query", "since"],
                    "message": "Field required",
                },
                {
                    "code": "missing",
                    "context": {},
                    "input": {"depth": 1, "granularity": granularity},
                    "location": ["query", "direction"],
                    "message": "Field required",
                },
                {
                    "code": "missing",
                    "context": {},
                    "input": {"depth": 1, "granularity": granularity},
                    "location": ["query", "start_node_id"],
                    "message": "Field required",
                },
            ],
            "message": "Invalid request",
        },
    }


@pytest.mark.parametrize(
    "entity_kind, start_node_id",
    [
        ("operations", generate_new_uuid()),
        ("datasets", 1),
        ("runs", generate_new_uuid()),
        ("jobs", 1),
    ],
    ids=["operations", "datasets", "runs", "jobs"],
)
async def test_get_lineage_missing_id(
    entity_kind: str,
    start_node_id: str,
    test_client: AsyncClient,
):
    since = datetime.now()

    response = await test_client.get(
        f"v1/{entity_kind}/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": start_node_id,
            "direction": "DOWNSTREAM",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [],
        "nodes": [],
    }


@pytest.mark.parametrize(
    "entity_kind, start_node_id",
    [("datasets", generate_new_uuid()), ("jobs", generate_new_uuid())],
    ids=["datasets", "jobs"],
)
async def test_get_lineage_start_node_id_int_type_validation(
    entity_kind: str,
    start_node_id: str,
    test_client: AsyncClient,
):
    since = datetime.now(tz=timezone.utc)
    response = await test_client.get(
        f"v1/{entity_kind}/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": start_node_id,
            "direction": "DOWNSTREAM",
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "details": [
                {
                    "code": "int_parsing",
                    "context": {},
                    "input": str(start_node_id),
                    "location": [
                        "query",
                        "start_node_id",
                    ],
                    "message": f"Input should be a valid integer, unable to parse string as an integer",
                },
            ],
            "message": "Invalid request",
        },
    }


@pytest.mark.parametrize(
    "entity_kind, start_node_id",
    [("operations", 1), ("runs", 1)],
    ids=["operations", "runs"],
)
async def test_get_lineage_start_node_id_uuid_type_validation(
    entity_kind: str,
    start_node_id: int,
    test_client: AsyncClient,
):
    since = datetime.now(tz=timezone.utc)
    response = await test_client.get(
        f"v1/{entity_kind}/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": start_node_id,
            "direction": "DOWNSTREAM",
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "details": [
                {
                    "code": "value_error",
                    "context": {},
                    "input": "1",
                    "location": [
                        "query",
                        "start_node_id",
                    ],
                    "message": "Value error, badly formed hexadecimal UUID string",
                },
            ],
            "message": "Invalid request",
        },
    }


async def test_get_lineage_until_less_than_since(test_client: AsyncClient):
    since = datetime.now(tz=timezone.utc)
    until = since - timedelta(days=1)

    response = await test_client.get(
        "v1/runs/lineage",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "start_node_id": str(generate_new_uuid()),
            "direction": "DOWNSTREAM",
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": ["query", "until"],
                    "code": "value_error",
                    "message": "Value error, 'since' should be less than 'until'",
                    "context": {},
                    "input": until.isoformat(),
                },
            ],
        },
    }


@pytest.mark.parametrize(
    ["depth", "error_type", "error_message", "context"],
    [
        (0, "greater_than_equal", "Input should be greater than or equal to 1", {"ge": 1}),
        (4, "less_than_equal", "Input should be less than or equal to 3", {"le": 3}),
    ],
    ids=["depth=0", "depth=4"],
)
async def test_get_lineage_depth_out_of_bounds(
    test_client: AsyncClient,
    depth: int,
    error_type: str,
    error_message: str,
    context: dict,
):
    since = datetime.now(tz=timezone.utc)
    response = await test_client.get(
        "v1/runs/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": str(generate_new_uuid()),
            "direction": "DOWNSTREAM",
            "depth": depth,
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": ["query", "depth"],
                    "code": error_type,
                    "message": error_message,
                    "context": context,
                    "input": str(depth),
                },
            ],
        },
    }
