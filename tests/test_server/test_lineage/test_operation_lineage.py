from datetime import timedelta
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from data_rentgen.db.models import Dataset, Interaction, Job, Location, Operation, Run
from tests.test_server.utils.enrich import enrich_datasets

pytestmark = [pytest.mark.server, pytest.mark.asyncio]

lineage_fixture_annotation = tuple[Job, list[Run], list[Dataset], list[Operation], list[Interaction]]


async def test_get_operation_lineage(
    test_client: AsyncClient,
    lineage: lineage_fixture_annotation,
    async_session: AsyncSession,
):
    _, runs, datasets, operations, _ = lineage
    operation = operations[1]

    datasets = await enrich_datasets(datasets, async_session)
    dataset = datasets[1]

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": runs[0].created_at.isoformat(),
            "point_kind": "OPERATION",
            "point_id": str(operation.id),
            "direction": "FROM",
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
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                },
            },
        ],
    }


operation_lineage_annotation = tuple[Operation, list[Dataset]]


async def get_operation_lineage_with_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    operation_to_datasets_lineage: operation_lineage_annotation,
):
    operation, datasets = operation_to_datasets_lineage
    datasets = await enrich_datasets(datasets, async_session)
    since = operation.created_at
    until = since + timedelta(seconds=1)

    dataset_nodes = [
        {
            "kind": "DATASET",
            "id": dataset.id,
            "format": dataset.format,
            "name": dataset.name,
            "location": {
                "name": dataset.location.name,
                "type": dataset.location.type,
                "addresses": [{"url": address.url} for address in dataset.location.addresses],
            },
        }
        for dataset in datasets[:2]
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
        },
    ]
    relations = [
        {
            "from": {"id": str(operation.id), "kind": "OPERATION"},
            "kind": "INTERACTION",
            "to": {"id": dataset.id, "kind": "DATASET"},
            "type": "APPEND",
        }
        for dataset in datasets[:2]
    ]

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "point_kind": "OPERATION",
            "point_id": str(operation.id),
            "direction": "FROM",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {"nodes": dataset_nodes + operation_nodes, "relations": relations}
