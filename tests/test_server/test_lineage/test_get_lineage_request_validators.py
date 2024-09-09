from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient

from data_rentgen.db.utils.uuid import generate_new_uuid

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_lineage_no_filter(test_client: AsyncClient):
    response = await test_client.get("v1/lineage")

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "details": [
                {
                    "code": "missing",
                    "context": {},
                    "input": None,
                    "location": ["query", "since"],
                    "message": "Field required",
                },
                {
                    "code": "missing",
                    "context": {},
                    "input": None,
                    "location": ["query", "point_kind"],
                    "message": "Field required",
                },
                {
                    "code": "missing",
                    "context": {},
                    "input": None,
                    "location": ["query", "point_id"],
                    "message": "Field required",
                },
                {
                    "code": "missing",
                    "context": {},
                    "input": None,
                    "location": ["query", "direction"],
                    "message": "Field required",
                },
            ],
            "message": "Invalid request",
        },
    }


@pytest.mark.parametrize(
    "point_kind, point_id",
    [
        ("OPERATION", generate_new_uuid()),
        ("DATASET", 1),
        ("RUN", generate_new_uuid()),
        ("JOB", 1),
    ],
    ids=["OPERATION", "DATASET", "RUN", "JOB"],
)
async def test_get_lineage_missing_id(
    point_kind: str,
    point_id: str,
    test_client: AsyncClient,
):
    since = datetime.now()

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": since.isoformat(),
            "point_kind": point_kind,
            "point_id": point_id,
            "direction": "FROM",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [],
        "nodes": [],
    }


@pytest.mark.parametrize(
    "point_kind, point_id",
    [("DATASET", generate_new_uuid()), ("JOB", generate_new_uuid())],
    ids=["DATASET", "JOB"],
)
async def test_get_lineage_point_id_int_type_validation(
    point_kind: str,
    point_id: str,
    test_client: AsyncClient,
):
    since = datetime.now(tz=timezone.utc)
    response = await test_client.get(
        "v1/lineage",
        params={
            "since": since.isoformat(),
            "point_kind": point_kind,
            "point_id": point_id,
            "direction": "FROM",
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
                    "input": {
                        "depth": 1,
                        "direction": "FROM",
                        "point_id": f"{point_id}",
                        "point_kind": f"{point_kind}",
                        "since": since.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "until": None,
                    },
                    "location": [],
                    "message": f"Value error, 'point_id' should be int for '{point_kind}' kind",
                },
            ],
            "message": "Invalid request",
        },
    }


@pytest.mark.parametrize(
    "point_kind, point_id",
    [("OPERATION", 1), ("RUN", 1)],
    ids=["OPERATION", "RUN"],
)
async def test_get_lineage_point_id_uuid_type_validation(
    point_kind: str,
    point_id: int,
    test_client: AsyncClient,
):
    since = datetime.now(tz=timezone.utc)
    response = await test_client.get(
        "v1/lineage",
        params={
            "since": since.isoformat(),
            "point_kind": point_kind,
            "point_id": point_id,
            "direction": "FROM",
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
                    "input": {
                        "depth": 1,
                        "direction": "FROM",
                        "point_id": point_id,
                        "point_kind": f"{point_kind}",
                        "since": since.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "until": None,
                    },
                    "location": [],
                    "message": f"Value error, 'point_id' should be UUIDv7 for '{point_kind}' kind",
                },
            ],
            "message": "Invalid request",
        },
    }


async def test_get_lineage_until_less_than_since(test_client: AsyncClient):
    since = datetime.now(tz=timezone.utc)
    until = since - timedelta(days=1)

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "point_kind": "RUN",
            "point_id": str(generate_new_uuid()),
            "direction": "FROM",
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
        "v1/lineage",
        params={
            "since": since.isoformat(),
            "point_kind": "RUN",
            "point_id": str(generate_new_uuid()),
            "direction": "FROM",
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
