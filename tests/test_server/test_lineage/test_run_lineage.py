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

LINEAGE_FIXTURE_ANNOTATION = tuple[list[Job], list[Run], list[Operation], list[Dataset], list[Interaction]]


async def test_get_run_lineage(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_same_run: LINEAGE_FIXTURE_ANNOTATION,
):
    jobs, runs, operations, datasets, _ = lineage_with_same_run

    jobs = await enrich_jobs(jobs, async_session)
    [run] = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": run.created_at.isoformat(),
            "point_kind": "RUN",
            "point_id": str(run.id),
            "direction": "FROM",
        },
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": run.job_id},
                "to": {"kind": "RUN", "id": str(run.id)},
                "type": None,
            },
        ]
        + [
            {
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(operation.run_id)},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
                "type": None,
            }
            for operation in operations
        ]
        + [
            {
                "kind": "INTERACTION",
                "from": {"kind": "OPERATION", "id": str(operation.id)},
                "to": {"kind": "DATASET", "id": dataset.id},
                "type": "APPEND",
            }
            for dataset, operation in zip(datasets, operations)
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
            }
            for job in jobs
        ]
        + [
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
            for dataset in datasets
        ]
        + [
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
                "started_by_user": {"name": run.started_by_user.name},
                "start_reason": run.start_reason.value,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run.end_reason,
            },
        ]
        + [
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
            for operation in operations
        ],
    }


async def test_get_run_lineage_with_direction_and_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_same_run: LINEAGE_FIXTURE_ANNOTATION,
):
    jobs, runs, all_operations, datasets, _ = lineage_with_same_run

    jobs = await enrich_jobs(jobs, async_session)
    [run] = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    since = run.created_at
    until = since + timedelta(seconds=1)
    operations = [operation for operation in all_operations if since <= operation.created_at <= until]

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "point_kind": "RUN",
            "point_id": str(run.id),
            "direction": "TO",
        },
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": run.job_id},
                "to": {"kind": "RUN", "id": str(run.id)},
                "type": None,
            },
        ]
        + [
            {
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(operation.run_id)},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
                "type": None,
            }
            for operation in operations
        ]
        + [
            {
                "kind": "INTERACTION",
                "from": {"kind": "DATASET", "id": dataset.id},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
                "type": "READ",
            }
            for operation, dataset in zip(operations, datasets)
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
            }
            for job in jobs
        ]
        + [
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
            for dataset in datasets
        ]
        + [
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
                "started_by_user": {"name": run.started_by_user.name},
                "start_reason": run.start_reason.value,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run.end_reason,
            },
        ]
        + [
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
            for operation in operations
        ],
    }
