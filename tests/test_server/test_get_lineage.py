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
                "position": operation.position,
                "description": operation.description,
                "started_at": operation.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ended_at": operation.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
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
                "position": operation.position,
                "description": operation.description,
                "started_at": operation.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ended_at": operation.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
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
        },
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(run.id)},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
                "type": None,
            },
            {
                "kind": "INTERACTION",
                "type": "APPEND",
                "from": {"kind": "OPERATION", "id": str(operation.id)},
                "to": {"kind": "DATASET", "id": dataset.id},
            },
        ],
        "nodes": [
            {
                "kind": "RUN",
                "id": str(run.id),
                "job_id": run.job_id,
                "parent_run_id": str(run.parent_run_id),
                "status": run.status.value,
                "external_id": run.external_id,
                "attempt": run.attempt,
                "persistent_log_url": run.persistent_log_url,
                "running_log_url": run.running_log_url,
                "started_at": run.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "started_by_user": None,
                "start_reason": run.start_reason.value,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run.end_reason,
            },
            {
                "kind": "OPERATION",
                "id": str(operation.id),
                "run_id": str(operation.run_id),
                "name": operation.name,
                "status": operation.status.value,
                "type": operation.type.value,
                "position": operation.position,
                "description": operation.description,
                "started_at": operation.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ended_at": operation.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
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
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(runs[0].id)},
                "to": {"kind": "OPERATION", "id": str(operations[1].id)},
                "type": None,
            },
            {
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(runs[1].id)},
                "to": {"kind": "OPERATION", "id": str(operations[3].id)},
                "type": None,
            },
            {
                "kind": "INTERACTION",
                "type": "APPEND",
                "from": {"kind": "OPERATION", "id": str(operations[1].id)},
                "to": {"kind": "DATASET", "id": datasets[1].id},
            },
            {
                "kind": "INTERACTION",
                "type": "APPEND",
                "from": {"kind": "OPERATION", "id": str(operations[3].id)},
                "to": {"kind": "DATASET", "id": datasets[3].id},
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
                "parent_run_id": str(runs[0].parent_run_id),
                "status": runs[0].status.value,
                "external_id": runs[0].external_id,
                "attempt": runs[0].attempt,
                "persistent_log_url": runs[0].persistent_log_url,
                "running_log_url": runs[0].running_log_url,
                "started_at": runs[0].started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "started_by_user": None,
                "start_reason": runs[0].start_reason.value,
                "ended_at": runs[0].ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": runs[0].end_reason,
            },
            {
                "kind": "RUN",
                "id": str(runs[1].id),
                "job_id": runs[1].job_id,
                "parent_run_id": str(runs[1].parent_run_id),
                "status": runs[1].status.value,
                "external_id": runs[1].external_id,
                "attempt": runs[1].attempt,
                "persistent_log_url": runs[1].persistent_log_url,
                "running_log_url": runs[1].running_log_url,
                "started_at": runs[1].started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "started_by_user": None,
                "start_reason": runs[1].start_reason.value,
                "ended_at": runs[1].ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": runs[1].end_reason,
            },
            {
                "kind": "OPERATION",
                "id": str(operations[1].id),
                "run_id": str(operations[1].run_id),
                "name": operations[1].name,
                "status": operations[1].status.value,
                "type": operations[1].type.value,
                "position": operations[1].position,
                "description": operations[1].description,
                "started_at": operations[1].started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ended_at": operations[1].ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
            {
                "kind": "OPERATION",
                "id": str(operations[3].id),
                "run_id": str(operations[3].run_id),
                "name": operations[3].name,
                "status": operations[3].status.value,
                "type": operations[3].type.value,
                "position": operations[3].position,
                "description": operations[3].description,
                "started_at": operations[3].started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ended_at": operations[3].ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
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
