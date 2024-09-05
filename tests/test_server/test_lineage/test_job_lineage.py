from datetime import timedelta
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from data_rentgen.db.models import Dataset, Interaction, Job, Location, Operation, Run
from tests.test_server.utils.enrich import enrich_datasets, enrich_jobs, enrich_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]

lineage_fixture_annotation = tuple[Job, list[Run], list[Dataset], list[Operation], list[Interaction]]


async def test_get_job_lineage(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage: lineage_fixture_annotation,
):
    job, runs, datasets, operations, _ = lineage

    jobs = await enrich_jobs([job], async_session)
    job = jobs[0]

    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": runs[0].created_at.isoformat(),
            "point_kind": "JOB",
            "point_id": job.id,
            "direction": "FROM",
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
                    "type": job.location.type,
                    "name": job.location.name,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
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
                    "name": datasets[1].location.name,
                    "type": datasets[1].location.type,
                    "addresses": [{"url": address.url} for address in datasets[1].location.addresses],
                },
            },
            {
                "kind": "DATASET",
                "id": datasets[3].id,
                "format": datasets[3].format,
                "name": datasets[3].name,
                "location": {
                    "name": datasets[3].location.name,
                    "type": datasets[3].location.type,
                    "addresses": [{"url": address.url} for address in datasets[3].location.addresses],
                },
            },
        ],
    }


async def test_get_job_lineage_with_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage: lineage_fixture_annotation,
):
    job, runs, datasets, operations, _ = lineage

    runs = await enrich_runs(runs, async_session)
    run = runs[0]

    datasets = await enrich_datasets(datasets, async_session)
    dataset = datasets[1]

    jobs = await enrich_jobs([job], async_session)
    job = jobs[0]
    operation = operations[1]

    since = run.created_at
    until = since + timedelta(seconds=1)
    job_node = {
        "kind": "JOB",
        "id": job.id,
        "name": job.name,
        "location": {
            "type": job.location.type,
            "name": job.location.name,
            "addresses": [{"url": address.url} for address in job.location.addresses],
        },
    }
    run_node = {
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
    }
    operation_node = {
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
    dataset_node = {
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
    relations = [
        {
            "from": {"id": job.id, "kind": "JOB"},
            "kind": "PARENT",
            "to": {"id": str(run.id), "kind": "RUN"},
            "type": None,
        },
        {
            "from": {"id": str(run.id), "kind": "RUN"},
            "kind": "PARENT",
            "to": {"id": str(operation.id), "kind": "OPERATION"},
            "type": None,
        },
        {
            "from": {"id": str(operation.id), "kind": "OPERATION"},
            "kind": "INTERACTION",
            "to": {"id": dataset.id, "kind": "DATASET"},
            "type": "APPEND",
        },
    ]

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "point_kind": "JOB",
            "point_id": job.id,
            "direction": "FROM",
        },
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "relations": relations,
        "nodes": [job_node, run_node, operation_node, dataset_node],
    }
