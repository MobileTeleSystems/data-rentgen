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
            "direction": "FROM",
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


dataset_lineage_annotation = tuple[Dataset, list[Operation]]


async def test_get_dataset_lineage_with_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    operations_to_dataset_lineage: dataset_lineage_annotation,
):
    dataset, operations = operations_to_dataset_lineage
    # Get location for dataset from db
    query = (
        select(Location)
        .join(Dataset, Dataset.location_id == Location.id)
        .options(selectinload(Location.addresses))
        .where(Dataset.id == dataset.id)
    )
    location = await async_session.scalar(query)
    since = operations[0].created_at
    until = since + timedelta(seconds=1)
    # Create expected results
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
    relations = [
        {
            "from": {"id": str(operation.id), "kind": "OPERATION"},
            "kind": "INTERACTION",
            "to": {"id": dataset.id, "kind": "DATASET"},
            "type": "APPEND",
        }
        for operation in operations[:2]
    ]

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "point_kind": "DATASET",
            "point_id": dataset.id,
            "direction": "TO",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {"nodes": dataset_nodes + operation_nodes, "relations": relations}
