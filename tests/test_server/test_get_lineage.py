from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from data_rentgen.db.models import Dataset, Interaction, Job, Location, Operation, Run

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
    _, runs, datasets, operations, _ = lineage
    operation = operations[0]
    dataset = datasets[0]
    # Get location for dataset from db
    query = (
        select(Location)
        .join(Dataset, Dataset.location_id == Location.id)
        .options(selectinload(Location.addresses))
        .where(Dataset.id == dataset.id)
    )
    location = await async_session.scalar(query)

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": runs[0].created_at.isoformat(),
            "point_kind": "DATASET",
            "point_id": dataset.id,
            "direction": "from",
            "granularity": "OPERATION",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "INTERACTION",
                "type": "READ",
                "from": {"kind": "DATASET", "id": dataset.id},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
            },
        ],
        "nodes": [
            {
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "name": location.name,
                    "type": location.type,
                    "addresses": [{"url": address.url} for address in location.addresses],
                },
            },
            {
                "kind": "OPERATION",
                "id": str(operation.id),
                "run_id": str(operation.run_id),
                "name": operation.name,
                "status": operation.status.value,
                "type": operation.type.value,
                "position": None,
                "description": None,
                "started_at": None,
                "ended_at": None,
            },
        ],
    }


async def test_get_operation_lineage(
    test_client: AsyncClient,
    lineage: lineage_fixture_annotation,
    async_session: AsyncSession,
):
    _, runs, datasets, operations, _ = lineage
    operation = operations[1]
    dataset = datasets[1]
    # Get location for dataset from db
    query = (
        select(Location)
        .join(Dataset, Dataset.location_id == Location.id)
        .options(selectinload(Location.addresses))
        .where(Dataset.id == dataset.id)
    )
    location = await async_session.scalar(query)

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": runs[0].created_at.isoformat(),
            "point_kind": "OPERATION",
            "point_id": operation.id,
            "direction": "from",
            "granularity": "OPERATION",
        },
    )
    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "INTERACTION",
                "type": "APPEND",
                "from": {"kind": "OPERATION", "id": str(operation.id)},
                "to": {"kind": "DATASET", "id": dataset.id},
            },
        ],
        "nodes": [
            {
                "kind": "OPERATION",
                "id": str(operation.id),
                "run_id": str(operation.run_id),
                "name": operation.name,
                "status": operation.status.value,
                "type": operation.type.value,
                "position": None,
                "description": None,
                "started_at": None,
                "ended_at": None,
            },
            {
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "name": location.name,
                    "type": location.type,
                    "addresses": [{"url": address.url} for address in location.addresses],
                },
            },
        ],
    }


async def test_get_run_lineage(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage: lineage_fixture_annotation,
):
    _, runs, datasets, operations, _ = lineage
    run = runs[0]
    operation = operations[1]
    dataset = datasets[1]
    # Get location for dataset from db
    query = (
        select(Location)
        .join(Dataset, Dataset.location_id == Location.id)
        .options(selectinload(Location.addresses))
        .where(Dataset.id == dataset.id)
    )
    location = await async_session.scalar(query)

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": runs[0].created_at.isoformat(),
            "point_kind": "RUN",
            "point_id": runs[0].id,
            "direction": "from",
            "granularity": "OPERATION",
        },
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "relations": [
            {
                "kind": "INTERACTION",
                "type": "APPEND",
                "from": {"kind": "OPERATION", "id": str(operation.id)},
                "to": {"kind": "DATASET", "id": dataset.id},
            },
            {
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(run.id)},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
                "type": None,
            },
        ],
        "nodes": [
            {
                "kind": "RUN",
                "id": str(run.id),
                "job_id": run.job_id,
                "parent_run_id": None,
                "status": run.status.value,
                "external_id": None,
                "attempt": None,
                "persistent_log_url": None,
                "running_log_url": None,
                "started_at": None,
                "started_by_user": None,
                "start_reason": None,
                "ended_at": None,
                "end_reason": None,
            },
            {
                "kind": "OPERATION",
                "id": str(operation.id),
                "run_id": str(operation.run_id),
                "name": operation.name,
                "status": operation.status.value,
                "type": operation.type.value,
                "position": None,
                "description": None,
                "started_at": None,
                "ended_at": None,
            },
            {
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "name": location.name,
                    "type": location.type,
                    "addresses": [{"url": address.url} for address in location.addresses],
                },
            },
        ],
    }


async def test_get_job_lineage(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage: lineage_fixture_annotation,
):
    job, runs, datasets, operations, _ = lineage
    # Get location for dataset from db
    query = select(Location).options(selectinload(Location.addresses)).where(Location.id == job.location_id)
    job_location = await async_session.scalar(query)
    query = (
        select(Location)
        .join(Dataset, Dataset.location_id == Location.id)
        .options(selectinload(Location.addresses))
        .where(Dataset.id == datasets[1].id)
    )
    location_1 = await async_session.scalar(query)
    query = (
        select(Location)
        .join(Dataset, Dataset.location_id == Location.id)
        .options(selectinload(Location.addresses))
        .where(Dataset.id == datasets[3].id)
    )
    location_3 = await async_session.scalar(query)

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": runs[0].created_at.isoformat(),
            "point_kind": "JOB",
            "point_id": job.id,
            "direction": "from",
            "granularity": "OPERATION",
        },
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": job.id},
                "to": {"kind": "RUN", "id": str(runs[0].id)},
                "type": None,
            },
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": job.id},
                "to": {"kind": "RUN", "id": str(runs[1].id)},
                "type": None,
            },
            {
                "kind": "INTERACTION",
                "type": "APPEND",
                "from": {"kind": "OPERATION", "id": str(operations[1].id)},
                "to": {"kind": "DATASET", "id": datasets[1].id},
            },
            {
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(runs[0].id)},
                "to": {"kind": "OPERATION", "id": str(operations[1].id)},
                "type": None,
            },
            {
                "kind": "INTERACTION",
                "type": "APPEND",
                "from": {"kind": "OPERATION", "id": str(operations[3].id)},
                "to": {"kind": "DATASET", "id": datasets[3].id},
            },
            {
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(runs[1].id)},
                "to": {"kind": "OPERATION", "id": str(operations[3].id)},
                "type": None,
            },
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "location": {
                    "type": job_location.type,
                    "name": job_location.name,
                    "addresses": [{"url": address.url} for address in job_location.addresses],
                },
            },
            {
                "kind": "RUN",
                "id": str(runs[0].id),
                "job_id": runs[0].job_id,
                "parent_run_id": None,
                "status": runs[0].status.value,
                "external_id": None,
                "attempt": None,
                "persistent_log_url": None,
                "running_log_url": None,
                "started_at": None,
                "started_by_user": None,
                "start_reason": None,
                "ended_at": None,
                "end_reason": None,
            },
            {
                "kind": "RUN",
                "id": str(runs[1].id),
                "job_id": runs[1].job_id,
                "parent_run_id": None,
                "status": runs[1].status.value,
                "external_id": None,
                "attempt": None,
                "persistent_log_url": None,
                "running_log_url": None,
                "started_at": None,
                "started_by_user": None,
                "start_reason": None,
                "ended_at": None,
                "end_reason": None,
            },
            {
                "kind": "OPERATION",
                "id": str(operations[1].id),
                "run_id": str(operations[1].run_id),
                "name": operations[1].name,
                "status": operations[1].status.value,
                "type": operations[1].type.value,
                "position": None,
                "description": None,
                "started_at": None,
                "ended_at": None,
            },
            {
                "kind": "DATASET",
                "id": datasets[1].id,
                "format": datasets[1].format,
                "name": datasets[1].name,
                "location": {
                    "name": location_1.name,
                    "type": location_1.type,
                    "addresses": [{"url": address.url} for address in location_1.addresses],
                },
            },
            {
                "kind": "OPERATION",
                "id": str(operations[3].id),
                "run_id": str(operations[3].run_id),
                "name": operations[3].name,
                "status": operations[3].status.value,
                "type": operations[3].type.value,
                "position": None,
                "description": None,
                "started_at": None,
                "ended_at": None,
            },
            {
                "kind": "DATASET",
                "id": datasets[3].id,
                "format": datasets[3].format,
                "name": datasets[3].name,
                "location": {
                    "name": location_3.name,
                    "type": location_3.type,
                    "addresses": [{"url": address.url} for address in location_3.addresses],
                },
            },
        ],
    }
