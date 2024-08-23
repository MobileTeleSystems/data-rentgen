from datetime import timedelta
from http import HTTPStatus

import pytest
from httpx import AsyncClient

from data_rentgen.db.models import Dataset, Interaction, Job, Operation, Run
from data_rentgen.db.utils.uuid import generate_new_uuid

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_lineage_no_filter(test_client: AsyncClient):

    response = await test_client.get("v1/lineage")

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY


lineage_fixture_annotation = tuple[Job, list[Run], list[Dataset], list[Operation], list[Interaction]]


@pytest.mark.parametrize(
    "point_kind, point_id",
    [
        ("OPERATION", generate_new_uuid()),
        ("DATASET", "1"),
        ("RUN", generate_new_uuid()),
        ("JOB", "1"),
    ],
    ids=["OPERATION", "DATASET", "RUN", "JOB"],
)
async def test_get_lineage_missing_id(
    point_kind: str,
    point_id: str,
    test_client: AsyncClient,
    lineage: lineage_fixture_annotation,
):
    _, runs, _, _, _ = lineage

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": runs[0].created_at.isoformat(),
            "point_kind": point_kind,
            "direction": "from",
            "point_id": point_id,
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "details": [
                {
                    "code": "model_attributes_type",
                    "context": {},
                    "input": None,
                    "location": [],
                    "message": "Input should be a valid dictionary or " "object to extract fields from",
                },
            ],
            "message": "Invalid request",
        },
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
    lineage: lineage_fixture_annotation,
):
    _, runs, _, _, _ = lineage

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": runs[0].created_at.isoformat(),
            "point_kind": point_kind,
            "direction": "from",
            "point_id": point_id,
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
                        "direction": "from",
                        "point_id": f"{point_id}",
                        "point_kind": f"{point_kind}",
                        "since": f'{runs[0].created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}',
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
    lineage: lineage_fixture_annotation,
):
    _, runs, _, _, _ = lineage

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": runs[0].created_at.isoformat(),
            "point_kind": point_kind,
            "direction": "from",
            "point_id": point_id,
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
                        "direction": "from",
                        "point_id": point_id,
                        "point_kind": f"{point_kind}",
                        "since": f'{runs[0].created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}',
                        "until": None,
                    },
                    "location": [],
                    "message": f"Value error, 'point_id' should be UUIDv7 for '{point_kind}' kind",
                },
            ],
            "message": "Invalid request",
        },
    }


async def test_get_lineage_until_less_than_since(
    test_client: AsyncClient,
    lineage: lineage_fixture_annotation,
):
    _, runs, _, _, _ = lineage
    since = runs[0].created_at
    until = since - timedelta(days=1)

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "point_kind": "RUN",
            "direction": "from",
            "point_id": runs[0].id,
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
