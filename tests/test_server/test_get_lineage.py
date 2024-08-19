from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select
from uuid6 import uuid7

from data_rentgen.db.models import Dataset, Interaction, Job, Operation, Run

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_lineage_empty(test_client: AsyncClient):
    response = await test_client.get("v1/lineage")

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY


lineage_fixture_annotation = tuple[Job, list[Run], list[Dataset], list[Operation], list[Interaction]]


async def test_get_dataset_lineage(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage: lineage_fixture_annotation,
):
    job, runs, datasets, operations, interactions = lineage

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": runs[0].created_at.isoformat(),
            "point_kind": "dataset",
            "point_id": datasets[0].id,
            "direction": "from",
            "granularity": "operation",
        },
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "relations": [
            {"type": "READ", "from_": datasets[0].id, "to": str(operations[0].id)},
        ],
        "nodes": [
            {
                "type": "operation",
                "id": str(operations[0].id),
                "name": operations[0].name,
                "status": "UNKNOWN",
                "operation_type": "BATCH",
            },
            {"type": "dataset", "id": datasets[0].id, "name": datasets[0].name},
        ],
    }


async def test_get_operation_lineage(test_client: AsyncClient, lineage: lineage_fixture_annotation):
    _, runs, datasets, operations, _ = lineage
    response = await test_client.get(
        "v1/lineage",
        params={
            "since": runs[0].created_at.isoformat(),
            "point_kind": "operation",
            "point_id": operations[1].id,
            "direction": "from",
            "granularity": "operation",
        },
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "relations": [
            {"type": "APPEND", "from_": str(operations[1].id), "to": datasets[1].id},
        ],
        "nodes": [
            {
                "type": "operation",
                "id": str(operations[1].id),
                "name": operations[1].name,
                "status": "UNKNOWN",
                "operation_type": "BATCH",
            },
            {"type": "dataset", "id": datasets[1].id, "name": datasets[1].name},
        ],
    }


async def test_get_run_lineage(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage: lineage_fixture_annotation,
):
    job, runs, datasets, operations, interactions = lineage

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": runs[0].created_at.isoformat(),
            "point_kind": "run",
            "point_id": runs[0].id,
            "direction": "from",
            "granularity": "operation",
        },
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "relations": [
            {"type": "parent", "from_": str(runs[0].id), "to": str(operations[1].id)},
            {"type": "APPEND", "from_": str(operations[1].id), "to": datasets[1].id},
        ],
        "nodes": [
            {"type": "run", "id": str(runs[0].id), "job_name": job.name, "status": "UNKNOWN"},
            {
                "type": "operation",
                "id": str(operations[1].id),
                "name": operations[1].name,
                "status": "UNKNOWN",
                "operation_type": "BATCH",
            },
            {"type": "dataset", "id": datasets[1].id, "name": datasets[1].name},
        ],
    }


async def test_get_job_lineage(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage: lineage_fixture_annotation,
):
    job, runs, datasets, operations, interactions = lineage
    response = await test_client.get(
        "v1/lineage",
        params={
            "since": runs[0].created_at.isoformat(),
            "point_kind": "job",
            "point_id": job.id,
            "direction": "from",
            "granularity": "operation",
        },
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "relations": [
            {"type": "parent", "from_": job.id, "to": str(runs[0].id)},
            {"type": "parent", "from_": str(runs[0].id), "to": str(operations[1].id)},
            {"type": "APPEND", "from_": str(operations[1].id), "to": datasets[1].id},
            {"type": "parent", "from_": job.id, "to": str(runs[1].id)},
            {"type": "parent", "from_": str(runs[1].id), "to": str(operations[3].id)},
            {"type": "APPEND", "from_": str(operations[3].id), "to": datasets[3].id},
        ],
        "nodes": [
            {"type": "job", "id": job.id, "name": job.name, "job_type": job.type},
            {"type": "run", "id": str(runs[0].id), "job_name": job.name, "status": "UNKNOWN"},
            {
                "type": "operation",
                "id": str(operations[1].id),
                "name": operations[1].name,
                "status": "UNKNOWN",
                "operation_type": "BATCH",
            },
            {"type": "dataset", "id": datasets[1].id, "name": datasets[1].name},
            {"type": "job", "id": job.id, "name": job.name, "job_type": job.type},
            {"type": "run", "id": str(runs[1].id), "job_name": job.name, "status": "UNKNOWN"},
            {
                "type": "operation",
                "id": str(operations[3].id),
                "name": operations[3].name,
                "status": "UNKNOWN",
                "operation_type": "BATCH",
            },
            {"type": "dataset", "id": datasets[3].id, "name": datasets[3].name},
        ],
    }
