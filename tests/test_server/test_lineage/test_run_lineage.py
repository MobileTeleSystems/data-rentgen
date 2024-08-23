from datetime import timedelta
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from data_rentgen.db.models import Dataset, Interaction, Job, Location, Operation, Run

pytestmark = [pytest.mark.server, pytest.mark.asyncio]

lineage_fixture_annotation = tuple[Job, list[Run], list[Dataset], list[Operation], list[Interaction]]


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
            "direction": "FROM",
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


run_lineage_annotation = tuple[Run, list[Dataset], list[Operation]]


async def test_get_run_lineage_with_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    run_lineage: run_lineage_annotation,
):
    run, datasets, operations = run_lineage
    # Get location for dataset from db
    query = (
        select(Location)
        .join(Dataset, Dataset.location_id == Location.id)
        .options(selectinload(Location.addresses))
        .where(Dataset.id == datasets[0].id)
    )
    location_0 = await async_session.scalar(query)
    query = (
        select(Location)
        .join(Dataset, Dataset.location_id == Location.id)
        .options(selectinload(Location.addresses))
        .where(Dataset.id == datasets[1].id)
    )
    location_1 = await async_session.scalar(query)
    locations = [location_0, location_1]
    since = run.created_at
    until = since + timedelta(seconds=1)
    # Create expected results
    run_nodes = [
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
    ]
    operation_nodes = [
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
        }
        for operation in operations[:2]
    ]
    dataset_nodes = [
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
        }
        for dataset, location in zip(datasets[:2], locations)
    ]
    run_relations = [
        {
            "kind": "PARENT",
            "from": {"kind": "RUN", "id": str(run.id)},
            "to": {"kind": "OPERATION", "id": str(operation.id)},
            "type": None,
        }
        for operation in operations[:2]
    ]
    operation_relations = [
        {
            "from": {"id": str(operation.id), "kind": "OPERATION"},
            "kind": "INTERACTION",
            "to": {"id": dataset.id, "kind": "DATASET"},
            "type": "APPEND",
        }
        for operation, dataset in zip(operations[:2], datasets[:2])
    ]

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "point_kind": "RUN",
            "point_id": run.id,
            "direction": "FROM",
        },
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "relations": run_relations + operation_relations,
        "nodes": run_nodes + operation_nodes + dataset_nodes,
    }
