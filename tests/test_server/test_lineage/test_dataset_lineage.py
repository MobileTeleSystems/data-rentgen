from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Dataset
from tests.test_server.utils.enrich import enrich_datasets, enrich_jobs, enrich_runs
from tests.test_server.utils.lineage_result import LineageResult
from tests.test_server.utils.stats import relation_stats_by_jobs, relation_stats_by_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio, pytest.mark.lineage]


async def test_get_dataset_lineage_unknown_id(
    test_client: AsyncClient,
    new_dataset: Dataset,
):
    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": datetime.now(tz=timezone.utc).isoformat(),
            "start_node_id": new_dataset.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [],
        "nodes": [],
    }


async def test_get_dataset_lineage_no_relations(
    test_client: AsyncClient,
    async_session: AsyncSession,
    dataset: Dataset,
):
    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": datetime.now(tz=timezone.utc).isoformat(),
            "start_node_id": dataset.id,
        },
    )

    [dataset] = await enrich_datasets([dataset], async_session)

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        # no symlinks, inputs or outputs, nothing to show in relations
        "relations": [],
        "nodes": [
            {
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "id": dataset.location.id,
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                    "external_id": dataset.location.external_id,
                },
            },
        ],
    }


async def test_get_dataset_lineage_with_granularity_run(
    test_client: AsyncClient,
    async_session: AsyncSession,
    three_days_lineage: LineageResult,
):
    lineage = three_days_lineage
    # We need a middle dataset, which has inputs and outputs
    dataset = lineage.datasets[1]

    inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    input_stats = relation_stats_by_runs(inputs)
    assert inputs

    outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]
    output_stats = relation_stats_by_runs(outputs)
    assert outputs

    run_ids = {input.run_id for input in inputs} | {output.run_id for output in outputs}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    [dataset] = await enrich_datasets([dataset], async_session)
    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": run.job_id},
                "to": {"kind": "RUN", "id": str(run.id)},
            }
            for run in sorted(runs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "RUN", "id": str(input.run_id)},
                "num_bytes": input_stats[(input.run_id, input.dataset_id)]["num_bytes"],
                "num_rows": input_stats[(input.run_id, input.dataset_id)]["num_rows"],
                "num_files": input_stats[(input.run_id, input.dataset_id)]["num_files"],
                "schema": None,
                "last_interaction_at": input_stats[(input.run_id, input.dataset_id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for input in sorted(inputs, key=lambda x: (x.dataset_id, x.run_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(output.run_id)},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": output.type,
                "num_bytes": output_stats[(output.run_id, output.dataset_id)]["num_bytes"],
                "num_rows": output_stats[(output.run_id, output.dataset_id)]["num_rows"],
                "num_files": output_stats[(output.run_id, output.dataset_id)]["num_files"],
                "schema": None,
                "last_interaction_at": output_stats[(output.run_id, output.dataset_id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for output in sorted(outputs, key=lambda x: (x.run_id, x.dataset_id))
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "id": job.location.id,
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
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
                    "id": dataset.location.id,
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                    "external_id": dataset.location.external_id,
                },
            },
        ]
        + [
            {
                "kind": "RUN",
                "id": str(run.id),
                "job_id": run.job_id,
                "created_at": run.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "parent_run_id": str(run.parent_run_id),
                "status": run.status.name,
                "external_id": run.external_id,
                "attempt": run.attempt,
                "persistent_log_url": run.persistent_log_url,
                "running_log_url": run.running_log_url,
                "started_at": run.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "started_by_user": {"name": run.started_by_user.name},
                "start_reason": run.start_reason.value,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run.end_reason,
            }
            for run in runs
        ],
    }


async def test_get_dataset_lineage_with_granularity_job(
    test_client: AsyncClient,
    async_session: AsyncSession,
    three_days_lineage: LineageResult,
):
    lineage = three_days_lineage
    # We need a middle dataset, which has inputs and outputs
    dataset = lineage.datasets[1]

    inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    input_stats = relation_stats_by_jobs(inputs)
    assert inputs

    outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]
    output_stats = relation_stats_by_jobs(outputs)
    assert outputs

    job_ids = {input.job_id for input in inputs} | {output.job_id for output in outputs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    [dataset] = await enrich_datasets([dataset], async_session)
    jobs = await enrich_jobs(jobs, async_session)
    since = min(run.created_at for run in lineage.runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
            "granularity": "JOB",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "JOB", "id": input.job_id},
                "num_bytes": input_stats[(input.job_id, input.dataset_id)]["num_bytes"],
                "num_rows": input_stats[(input.job_id, input.dataset_id)]["num_rows"],
                "num_files": input_stats[(input.job_id, input.dataset_id)]["num_files"],
                "schema": None,
                "last_interaction_at": input_stats[(input.job_id, input.dataset_id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for input in sorted(inputs, key=lambda x: (x.dataset_id, x.job_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "JOB", "id": output.job_id},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": output.type,
                "num_bytes": output_stats[(output.job_id, output.dataset_id)]["num_bytes"],
                "num_rows": output_stats[(output.job_id, output.dataset_id)]["num_rows"],
                "num_files": output_stats[(output.job_id, output.dataset_id)]["num_files"],
                "schema": None,
                "last_interaction_at": output_stats[(output.job_id, output.dataset_id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for output in sorted(outputs, key=lambda x: (x.job_id, x.dataset_id))
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "id": job.location.id,
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
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
                    "id": dataset.location.id,
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                    "external_id": dataset.location.external_id,
                },
            },
        ],
    }


async def test_get_dataset_lineage_with_granularity_operation(
    test_client: AsyncClient,
    async_session: AsyncSession,
    three_days_lineage: LineageResult,
):
    lineage = three_days_lineage
    # We need a middle dataset, which has inputs and outputs
    dataset = lineage.datasets[1]

    inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]
    assert outputs

    operation_ids = {input.operation_id for input in inputs} | {output.operation_id for output in outputs}
    operations = [operation for operation in lineage.operations if operation.id in operation_ids]
    assert operations

    run_ids = {operation.run_id for operation in operations}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    [dataset] = await enrich_datasets([dataset], async_session)
    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
            "granularity": "OPERATION",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": run.job_id},
                "to": {"kind": "RUN", "id": str(run.id)},
            }
            for run in sorted(runs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(operation.run_id)},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
            }
            for operation in sorted(operations, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "OPERATION", "id": str(input.operation_id)},
                "num_bytes": input.num_bytes,
                "num_rows": input.num_rows,
                "num_files": input.num_files,
                "schema": {
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in input.schema.fields
                    ],
                },
                "last_interaction_at": input.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for input in sorted(inputs, key=lambda x: (x.dataset_id, x.operation_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "OPERATION", "id": str(output.operation_id)},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": output.type,
                "num_bytes": output.num_bytes,
                "num_rows": output.num_rows,
                "num_files": output.num_files,
                "schema": {
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in output.schema.fields
                    ],
                },
                "last_interaction_at": output.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for output in sorted(outputs, key=lambda x: (x.operation_id, x.dataset_id))
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "id": job.location.id,
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
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
                    "id": dataset.location.id,
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                    "external_id": dataset.location.external_id,
                },
            },
        ]
        + [
            {
                "kind": "RUN",
                "id": str(run.id),
                "job_id": run.job_id,
                "created_at": run.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "parent_run_id": str(run.parent_run_id),
                "status": run.status.name,
                "external_id": run.external_id,
                "attempt": run.attempt,
                "persistent_log_url": run.persistent_log_url,
                "running_log_url": run.running_log_url,
                "started_at": run.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "started_by_user": {"name": run.started_by_user.name},
                "start_reason": run.start_reason.value,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run.end_reason,
            }
            for run in runs
        ]
        + [
            {
                "kind": "OPERATION",
                "id": str(operation.id),
                "created_at": operation.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "run_id": str(operation.run_id),
                "name": operation.name,
                "status": operation.status.name,
                "type": operation.type.value,
                "position": operation.position,
                "group": operation.group,
                "description": operation.description,
                "started_at": operation.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ended_at": operation.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            for operation in operations
        ],
    }


async def test_get_dataset_lineage_with_direction_downstream(
    test_client: AsyncClient,
    async_session: AsyncSession,
    three_days_lineage: LineageResult,
):
    lineage = three_days_lineage
    # We need a middle dataset, which has inputs and outputs
    dataset = lineage.datasets[1]

    inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    input_stats = relation_stats_by_runs(inputs)
    assert inputs

    run_ids = {input.run_id for input in inputs}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    [dataset] = await enrich_datasets([dataset], async_session)
    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    since = min(input.created_at for input in inputs)

    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
            "direction": "DOWNSTREAM",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": run.job_id},
                "to": {"kind": "RUN", "id": str(run.id)},
            }
            for run in runs
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "RUN", "id": str(input.run_id)},
                "num_bytes": input_stats[(input.run_id, input.dataset_id)]["num_bytes"],
                "num_rows": input_stats[(input.run_id, input.dataset_id)]["num_rows"],
                "num_files": input_stats[(input.run_id, input.dataset_id)]["num_files"],
                "schema": None,
                "last_interaction_at": input_stats[(input.run_id, input.dataset_id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for input in sorted(inputs, key=lambda x: (x.dataset_id, x.run_id))
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "id": job.location.id,
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            }
            for job in sorted(jobs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "id": dataset.location.id,
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                    "external_id": dataset.location.external_id,
                },
            },
        ]
        + [
            {
                "kind": "RUN",
                "id": str(run.id),
                "job_id": run.job_id,
                "created_at": run.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "parent_run_id": str(run.parent_run_id),
                "status": run.status.name,
                "external_id": run.external_id,
                "attempt": run.attempt,
                "persistent_log_url": run.persistent_log_url,
                "running_log_url": run.running_log_url,
                "started_at": run.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "started_by_user": {"name": run.started_by_user.name},
                "start_reason": run.start_reason.value,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run.end_reason,
            }
            for run in sorted(runs, key=lambda x: x.id)
        ],
    }


async def test_get_dataset_lineage_with_direction_upstream(
    test_client: AsyncClient,
    async_session: AsyncSession,
    three_days_lineage: LineageResult,
):
    lineage = three_days_lineage
    # We need a middle dataset, which has inputs and outputs
    dataset = lineage.datasets[1]

    outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]
    output_stats = relation_stats_by_runs(outputs)
    assert outputs

    run_ids = {output.run_id for output in outputs}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    [dataset] = await enrich_datasets([dataset], async_session)
    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    since = min(output.created_at for output in outputs)

    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
            "direction": "UPSTREAM",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": run.job_id},
                "to": {"kind": "RUN", "id": str(run.id)},
            }
            for run in runs
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(output.run_id)},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": output.type,
                "num_bytes": output_stats[(output.run_id, output.dataset_id)]["num_bytes"],
                "num_rows": output_stats[(output.run_id, output.dataset_id)]["num_rows"],
                "num_files": output_stats[(output.run_id, output.dataset_id)]["num_files"],
                "schema": None,
                "last_interaction_at": output_stats[(output.run_id, output.dataset_id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for output in sorted(outputs, key=lambda x: (x.run_id, x.dataset_id))
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "id": job.location.id,
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            }
            for job in sorted(jobs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "id": dataset.location.id,
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                    "external_id": dataset.location.external_id,
                },
            },
        ]
        + [
            {
                "kind": "RUN",
                "id": str(run.id),
                "job_id": run.job_id,
                "created_at": run.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "parent_run_id": str(run.parent_run_id),
                "status": run.status.name,
                "external_id": run.external_id,
                "attempt": run.attempt,
                "persistent_log_url": run.persistent_log_url,
                "running_log_url": run.running_log_url,
                "started_at": run.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "started_by_user": {"name": run.started_by_user.name},
                "start_reason": run.start_reason.value,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run.end_reason,
            }
            for run in sorted(runs, key=lambda x: x.id)
        ],
    }


async def test_get_dataset_lineage_with_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    three_days_lineage: LineageResult,
):
    lineage = three_days_lineage
    # We need a middle dataset, which has inputs and outputs
    dataset = lineage.datasets[1]

    inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]

    since = min([input.created_at for input in inputs] + [output.created_at for output in outputs])
    until = since + timedelta(seconds=0.1)

    inputs = [input for input in inputs if since <= input.created_at <= until]
    input_stats = relation_stats_by_runs(inputs)
    assert inputs

    outputs = [output for output in outputs if since <= output.created_at <= until]
    output_stats = relation_stats_by_runs(outputs)
    assert outputs

    run_ids = {input.run_id for input in inputs} | {output.run_id for output in outputs}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    [dataset] = await enrich_datasets([dataset], async_session)
    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)

    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "start_node_id": dataset.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": run.job_id},
                "to": {"kind": "RUN", "id": str(run.id)},
            }
            for run in runs
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "RUN", "id": str(input.run_id)},
                "num_bytes": input_stats[(input.run_id, input.dataset_id)]["num_bytes"],
                "num_rows": input_stats[(input.run_id, input.dataset_id)]["num_rows"],
                "num_files": input_stats[(input.run_id, input.dataset_id)]["num_files"],
                "schema": None,
                "last_interaction_at": input_stats[(input.run_id, input.dataset_id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for input in sorted(inputs, key=lambda x: (x.dataset_id, x.run_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(output.run_id)},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": output.type,
                "num_bytes": output_stats[(output.run_id, output.dataset_id)]["num_bytes"],
                "num_rows": output_stats[(output.run_id, output.dataset_id)]["num_rows"],
                "num_files": output_stats[(output.run_id, output.dataset_id)]["num_files"],
                "schema": None,
                "last_interaction_at": output_stats[(output.run_id, output.dataset_id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for output in sorted(outputs, key=lambda x: (x.run_id, x.dataset_id))
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "id": job.location.id,
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            }
            for job in sorted(jobs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "id": dataset.location.id,
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                    "external_id": dataset.location.external_id,
                },
            },
        ]
        + [
            {
                "kind": "RUN",
                "id": str(run.id),
                "job_id": run.job_id,
                "created_at": run.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "parent_run_id": str(run.parent_run_id),
                "status": run.status.name,
                "external_id": run.external_id,
                "attempt": run.attempt,
                "persistent_log_url": run.persistent_log_url,
                "running_log_url": run.running_log_url,
                "started_at": run.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "started_by_user": {"name": run.started_by_user.name},
                "start_reason": run.start_reason.value,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run.end_reason,
            }
            for run in sorted(runs, key=lambda x: x.id)
        ],
    }


async def test_get_dataset_lineage_with_depth(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LineageResult,
):
    lineage = lineage_with_depth
    # Select only relations marked with *
    # J1 -*> R1, D1 -*> R1 -*> D2
    # J2 -*> R2, D2 -*> R2 --> D3
    # J3 --> R3, D3 --> R3 --> D4

    # Go dataset -> runs[first level]
    first_level_dataset = lineage.datasets[0]
    first_level_inputs = [input for input in lineage.inputs if input.dataset_id == first_level_dataset.id]
    first_level_outputs = [output for output in lineage.outputs if output.dataset_id == first_level_dataset.id]
    first_level_input_run_ids = {input.run_id for input in first_level_inputs}
    first_level_output_run_ids = {output.run_id for output in first_level_outputs}
    first_level_run_ids = first_level_input_run_ids | first_level_output_run_ids
    assert first_level_run_ids

    # Go runs[first level] -> datasets[second level]
    second_level_inputs = [input for input in lineage.inputs if input.run_id in first_level_output_run_ids]
    second_level_outputs = [output for output in lineage.outputs if output.run_id in first_level_input_run_ids]
    second_level_input_dataset_ids = {input.dataset_id for input in second_level_inputs}
    second_level_output_dataset_ids = {output.dataset_id for output in second_level_outputs}
    second_level_dataset_ids = second_level_input_dataset_ids | second_level_output_dataset_ids - {
        first_level_dataset.id,
    }
    assert second_level_dataset_ids

    # Go datasets[second level] -> runs[third level]
    # There are more levels in this graph, but we stop here
    third_level_inputs = [input for input in lineage.inputs if input.dataset_id in second_level_output_dataset_ids]
    third_level_outputs = [output for output in lineage.outputs if output.dataset_id in second_level_input_dataset_ids]
    third_level_input_run_ids = {input.run_id for input in third_level_inputs}
    third_level_output_run_ids = {output.run_id for output in third_level_outputs}
    third_level_run_ids = third_level_input_run_ids | third_level_output_run_ids - first_level_run_ids
    assert third_level_run_ids

    inputs = first_level_inputs + second_level_inputs + third_level_inputs
    input_stats = relation_stats_by_runs(inputs)
    assert inputs

    outputs = first_level_outputs + second_level_outputs + third_level_outputs
    output_stats = relation_stats_by_runs(outputs)
    assert outputs

    dataset_ids = {first_level_dataset.id} | second_level_dataset_ids
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    run_ids = first_level_run_ids | third_level_run_ids
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": first_level_dataset.id,
            "depth": 3,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": run.job_id},
                "to": {"kind": "RUN", "id": str(run.id)},
            }
            for run in sorted(runs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "RUN", "id": str(input.run_id)},
                "num_bytes": input_stats[(input.run_id, input.dataset_id)]["num_bytes"],
                "num_rows": input_stats[(input.run_id, input.dataset_id)]["num_rows"],
                "num_files": input_stats[(input.run_id, input.dataset_id)]["num_files"],
                "schema": None,
                "last_interaction_at": input_stats[(input.run_id, input.dataset_id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for input in sorted(inputs, key=lambda x: (x.dataset_id, x.run_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(output.run_id)},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": output.type,
                "num_bytes": output_stats[(output.run_id, output.dataset_id)]["num_bytes"],
                "num_rows": output_stats[(output.run_id, output.dataset_id)]["num_rows"],
                "num_files": output_stats[(output.run_id, output.dataset_id)]["num_files"],
                "schema": None,
                "last_interaction_at": output_stats[(output.run_id, output.dataset_id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for output in sorted(outputs, key=lambda x: (x.run_id, x.dataset_id))
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "id": job.location.id,
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            }
            for job in sorted(jobs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "id": dataset.location.id,
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                    "external_id": dataset.location.external_id,
                },
            }
            for dataset in sorted(datasets, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "RUN",
                "id": str(run.id),
                "job_id": run.job_id,
                "created_at": run.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "parent_run_id": str(run.parent_run_id),
                "status": run.status.name,
                "external_id": run.external_id,
                "attempt": run.attempt,
                "persistent_log_url": run.persistent_log_url,
                "running_log_url": run.running_log_url,
                "started_at": run.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "started_by_user": {"name": run.started_by_user.name},
                "start_reason": run.start_reason.value,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run.end_reason,
            }
            for run in sorted(runs, key=lambda x: x.id)
        ],
    }


async def test_get_dataset_lineage_with_depth_and_granularity_job(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LineageResult,
):
    lineage = lineage_with_depth
    # Select only relations marked with *
    # D1 -*> J1 -*> D2
    # D2 -*> J2 --> D3
    # D3 --> J3 --> D4

    # Go dataset[first level] -> jobs[first level]
    first_level_dataset = lineage.datasets[0]
    first_level_inputs = [input for input in lineage.inputs if input.dataset_id == first_level_dataset.id]
    first_level_outputs = [output for output in lineage.outputs if output.dataset_id == first_level_dataset.id]
    first_level_input_job_ids = {input.job_id for input in first_level_inputs}
    first_level_output_job_ids = {output.job_id for output in first_level_outputs}
    first_level_job_ids = first_level_input_job_ids | first_level_output_job_ids
    assert first_level_job_ids

    # Go job[first level] -> datasets[second level]
    second_level_inputs = [input for input in lineage.inputs if input.job_id in first_level_output_job_ids]
    second_level_outputs = [output for output in lineage.outputs if output.job_id in first_level_input_job_ids]
    second_level_input_dataset_ids = {input.dataset_id for input in second_level_inputs}
    second_level_output_dataset_ids = {output.dataset_id for output in second_level_outputs}
    second_level_dataset_ids = second_level_input_dataset_ids | second_level_output_dataset_ids
    assert second_level_dataset_ids

    # Go datasets[second level] -> jobs[third level]
    third_level_inputs = [input for input in lineage.inputs if input.dataset_id in second_level_output_dataset_ids]
    third_level_outputs = [output for output in lineage.outputs if output.dataset_id in second_level_input_dataset_ids]
    third_level_input_job_ids = {input.job_id for input in third_level_inputs}
    third_level_output_job_ids = {output.job_id for output in third_level_outputs}
    third_level_job_ids = third_level_input_job_ids | third_level_output_job_ids - first_level_job_ids
    assert third_level_job_ids

    inputs = first_level_inputs + second_level_inputs + third_level_inputs
    input_stats = relation_stats_by_jobs(inputs)
    assert inputs

    outputs = first_level_outputs + second_level_outputs + third_level_outputs
    output_stats = relation_stats_by_jobs(outputs)
    assert outputs

    dataset_ids = {first_level_dataset.id} | second_level_dataset_ids
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    job_ids = first_level_job_ids | third_level_job_ids
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    datasets = await enrich_datasets(datasets, async_session)
    jobs = await enrich_jobs(jobs, async_session)
    since = min(run.created_at for run in lineage.runs if run.job_id in job_ids)

    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": first_level_dataset.id,
            "depth": 3,
            "granularity": "JOB",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "JOB", "id": input.job_id},
                "num_bytes": input_stats[(input.job_id, input.dataset_id)]["num_bytes"],
                "num_rows": input_stats[(input.job_id, input.dataset_id)]["num_rows"],
                "num_files": input_stats[(input.job_id, input.dataset_id)]["num_files"],
                "schema": None,
                "last_interaction_at": input_stats[(input.job_id, input.dataset_id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for input in sorted(inputs, key=lambda x: (x.dataset_id, x.job_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "JOB", "id": output.job_id},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": output.type,
                "num_bytes": output_stats[(output.job_id, output.dataset_id)]["num_bytes"],
                "num_rows": output_stats[(output.job_id, output.dataset_id)]["num_rows"],
                "num_files": output_stats[(output.job_id, output.dataset_id)]["num_files"],
                "schema": None,
                "last_interaction_at": output_stats[(output.job_id, output.dataset_id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for output in sorted(outputs, key=lambda x: (x.job_id, x.dataset_id))
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "id": job.location.id,
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            }
            for job in sorted(jobs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "id": dataset.location.id,
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                    "external_id": dataset.location.external_id,
                },
            }
            for dataset in sorted(datasets, key=lambda x: x.id)
        ],
    }


async def test_get_dataset_lineage_with_depth_and_granularity_operation(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LineageResult,
):
    lineage = lineage_with_depth
    # Select only relations marked with *
    # J1 -*> R1 -*> O1, D1 -*> O1 -*> D2
    # J2 -*> R2 -*> O2, D2 -*> O2 --> D3
    # J3 --> R3 --> O3, D3 --> O3 --> D4

    # Go datasets[first level] -> operations[first level] + runs[first level] + jobs[first level]
    first_level_dataset = lineage.datasets[0]
    first_level_inputs = [input for input in lineage.inputs if input.dataset_id == first_level_dataset.id]
    first_level_outputs = [output for output in lineage.outputs if output.dataset_id == first_level_dataset.id]
    first_level_input_operation_ids = {input.operation_id for input in first_level_inputs}
    first_level_output_operation_ids = {output.operation_id for output in first_level_outputs}
    first_level_operation_ids = first_level_input_operation_ids | first_level_output_operation_ids
    assert first_level_operation_ids

    # Go operations[first level] -> datasets[second level]
    second_level_inputs = [input for input in lineage.inputs if input.operation_id in first_level_output_operation_ids]
    second_level_outputs = [
        output for output in lineage.outputs if output.operation_id in first_level_input_operation_ids
    ]
    second_level_input_dataset_ids = {input.dataset_id for input in second_level_inputs}
    second_level_output_dataset_ids = {output.dataset_id for output in second_level_outputs}
    second_level_dataset_ids = second_level_input_dataset_ids | second_level_output_dataset_ids
    assert second_level_dataset_ids

    # Go datasets[second level] -> operations[third level] + runs[third level] + jobs[third level]
    third_level_inputs = [input for input in lineage.inputs if input.dataset_id in second_level_output_dataset_ids]
    third_level_outputs = [output for output in lineage.outputs if output.dataset_id in second_level_input_dataset_ids]
    third_level_input_operation_ids = {input.operation_id for input in third_level_inputs}
    third_level_output_operation_ids = {output.operation_id for output in third_level_outputs}
    third_level_operation_ids = (
        third_level_input_operation_ids | third_level_output_operation_ids - first_level_operation_ids
    )
    assert third_level_operation_ids

    inputs = first_level_inputs + second_level_inputs + third_level_inputs
    assert inputs

    outputs = first_level_outputs + second_level_outputs + third_level_outputs
    assert outputs

    dataset_ids = {first_level_dataset.id} | second_level_dataset_ids
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    operation_ids = first_level_operation_ids | third_level_operation_ids
    operations = [operation for operation in lineage.operations if operation.id in operation_ids]
    assert operations

    run_ids = {input.run_id for input in inputs}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {input.job_id for input in inputs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    datasets = await enrich_datasets(datasets, async_session)
    runs = await enrich_runs(runs, async_session)
    jobs = await enrich_jobs(jobs, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": first_level_dataset.id,
            "depth": 3,
            "granularity": "OPERATION",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": run.job_id},
                "to": {"kind": "RUN", "id": str(run.id)},
            }
            for run in sorted(runs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(operation.run_id)},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
            }
            for operation in sorted(operations, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "OPERATION", "id": str(input.operation_id)},
                "num_bytes": input.num_bytes,
                "num_rows": input.num_rows,
                "num_files": input.num_files,
                "schema": {
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in input.schema.fields
                    ],
                },
                "last_interaction_at": input.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for input in sorted(inputs, key=lambda x: (x.dataset_id, x.operation_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "OPERATION", "id": str(output.operation_id)},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": output.type,
                "num_bytes": output.num_bytes,
                "num_rows": output.num_rows,
                "num_files": output.num_files,
                "schema": {
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in output.schema.fields
                    ],
                },
                "last_interaction_at": output.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for output in sorted(outputs, key=lambda x: (x.operation_id, x.dataset_id))
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "id": job.location.id,
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            }
            for job in sorted(jobs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "id": dataset.location.id,
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                    "external_id": dataset.location.external_id,
                },
            }
            for dataset in sorted(datasets, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "RUN",
                "id": str(run.id),
                "job_id": run.job_id,
                "created_at": run.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "parent_run_id": str(run.parent_run_id),
                "status": run.status.name,
                "external_id": run.external_id,
                "attempt": run.attempt,
                "persistent_log_url": run.persistent_log_url,
                "running_log_url": run.running_log_url,
                "started_at": run.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "started_by_user": {"name": run.started_by_user.name},
                "start_reason": run.start_reason.value,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run.end_reason,
            }
            for run in sorted(runs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "OPERATION",
                "id": str(operation.id),
                "created_at": operation.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "run_id": str(operation.run_id),
                "name": operation.name,
                "status": operation.status.name,
                "type": operation.type.value,
                "position": operation.position,
                "group": operation.group,
                "description": operation.description,
                "started_at": operation.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ended_at": operation.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            for operation in sorted(operations, key=lambda x: x.id)
        ],
    }


async def test_get_dataset_lineage_with_depth_ignore_cycles(
    test_client: AsyncClient,
    async_session: AsyncSession,
    cyclic_lineage: LineageResult,
):
    lineage = cyclic_lineage
    # Select all relations:
    # J1 -*> R1 -*> O1, D1 -*> O1 -*> D2
    # J2 -*> R2 -*> O2, D2 -*> O2 -*> D1

    # We can start at any dataset
    dataset = lineage.datasets[0]

    input_stats = relation_stats_by_runs(lineage.inputs)
    output_stats = relation_stats_by_runs(lineage.outputs)

    datasets = await enrich_datasets(lineage.datasets, async_session)
    jobs = await enrich_jobs(lineage.jobs, async_session)
    runs = await enrich_runs(lineage.runs, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
            "depth": 3,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": run.job_id},
                "to": {"kind": "RUN", "id": str(run.id)},
            }
            for run in sorted(runs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "RUN", "id": str(input.run_id)},
                "num_bytes": input_stats[(input.run_id, input.dataset_id)]["num_bytes"],
                "num_rows": input_stats[(input.run_id, input.dataset_id)]["num_rows"],
                "num_files": input_stats[(input.run_id, input.dataset_id)]["num_files"],
                "schema": None,
                "last_interaction_at": input_stats[(input.run_id, input.dataset_id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for input in sorted(lineage.inputs, key=lambda x: (x.dataset_id, x.run_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(output.run_id)},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": output.type,
                "num_bytes": output_stats[(output.run_id, output.dataset_id)]["num_bytes"],
                "num_rows": output_stats[(output.run_id, output.dataset_id)]["num_rows"],
                "num_files": output_stats[(output.run_id, output.dataset_id)]["num_files"],
                "schema": None,
                "last_interaction_at": output_stats[(output.run_id, output.dataset_id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for output in sorted(lineage.outputs, key=lambda x: (x.run_id, x.dataset_id))
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "id": job.location.id,
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            }
            for job in sorted(jobs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "id": dataset.location.id,
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                    "external_id": dataset.location.external_id,
                },
            }
            for dataset in sorted(datasets, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "RUN",
                "id": str(run.id),
                "job_id": run.job_id,
                "created_at": run.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "parent_run_id": str(run.parent_run_id),
                "status": run.status.name,
                "external_id": run.external_id,
                "attempt": run.attempt,
                "persistent_log_url": run.persistent_log_url,
                "running_log_url": run.running_log_url,
                "started_at": run.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "started_by_user": {"name": run.started_by_user.name},
                "start_reason": run.start_reason.value,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run.end_reason,
            }
            for run in sorted(runs, key=lambda x: x.id)
        ],
    }


async def test_get_dataset_lineage_with_depth_ignore_unrelated_datasets(
    test_client: AsyncClient,
    async_session: AsyncSession,
    branchy_lineage: LineageResult,
):
    lineage = branchy_lineage
    # Start from D3, build lineage with direction=BOTH
    initial_dataset = lineage.datasets[3]

    # Select only relations marked with *
    #     D0   D1
    #      *\ /*
    # J0 -*> R0 -> D2
    #        *\
    #         D3  D4
    #          *\ /
    #     J1 -*> R1 -*> D5
    #            *\
    #             D6  D7
    #              *\ /
    #         J2 -*> R2 -> D8
    #                 \
    #                  D9

    datasets = [
        lineage.datasets[0],
        lineage.datasets[1],
        # D2 is not a part of (D3,R1,D0,D1) input chain
        lineage.datasets[3],
        # D4 is not a part of (D3,R1,D0,D1) input chain
        lineage.datasets[5],
        lineage.datasets[6],
        # D7 is not a part of (D3,R1,D6) input chain
        # D8, D9 exceed depth=3
    ]
    dataset_ids = {dataset.id for dataset in datasets}

    inputs = [input for input in lineage.inputs if input.dataset_id in dataset_ids]
    input_stats = relation_stats_by_runs(inputs)
    assert inputs

    outputs = [output for output in lineage.outputs if output.dataset_id in dataset_ids]
    output_stats = relation_stats_by_runs(outputs)
    assert outputs

    jobs = await enrich_jobs(lineage.jobs, async_session)
    runs = await enrich_runs(lineage.runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": initial_dataset.id,
            "depth": 3,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": run.job_id},
                "to": {"kind": "RUN", "id": str(run.id)},
            }
            for run in sorted(runs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "RUN", "id": str(input.run_id)},
                "num_bytes": input_stats[(input.run_id, input.dataset_id)]["num_bytes"],
                "num_rows": input_stats[(input.run_id, input.dataset_id)]["num_rows"],
                "num_files": input_stats[(input.run_id, input.dataset_id)]["num_files"],
                "schema": None,
                "last_interaction_at": input_stats[(input.run_id, input.dataset_id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for input in sorted(inputs, key=lambda x: (x.dataset_id, x.run_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(output.run_id)},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": output.type,
                "num_bytes": output_stats[(output.run_id, output.dataset_id)]["num_bytes"],
                "num_rows": output_stats[(output.run_id, output.dataset_id)]["num_rows"],
                "num_files": output_stats[(output.run_id, output.dataset_id)]["num_files"],
                "schema": None,
                "last_interaction_at": output_stats[(output.run_id, output.dataset_id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for output in sorted(outputs, key=lambda x: (x.run_id, x.dataset_id))
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "id": job.location.id,
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            }
            for job in sorted(jobs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "id": dataset.location.id,
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                    "external_id": dataset.location.external_id,
                },
            }
            for dataset in sorted(datasets, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "RUN",
                "id": str(run.id),
                "job_id": run.job_id,
                "created_at": run.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "parent_run_id": str(run.parent_run_id),
                "status": run.status.name,
                "external_id": run.external_id,
                "attempt": run.attempt,
                "persistent_log_url": run.persistent_log_url,
                "running_log_url": run.running_log_url,
                "started_at": run.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "started_by_user": {"name": run.started_by_user.name},
                "start_reason": run.start_reason.value,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run.end_reason,
            }
            for run in sorted(runs, key=lambda x: x.id)
        ],
    }


async def test_get_dataset_lineage_with_symlink(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_symlinks: LineageResult,
):
    lineage = lineage_with_symlinks
    # Start from dataset between J0 and J1 lineages
    initial_dataset = lineage.datasets[1]

    dataset_symlinks = [
        dataset_symlink
        for dataset_symlink in lineage.dataset_symlinks
        if dataset_symlink.from_dataset_id == initial_dataset.id or dataset_symlink.to_dataset_id == initial_dataset.id
    ]
    dataset_ids_from_symlink = {dataset_symlink.from_dataset_id for dataset_symlink in dataset_symlinks}
    dataset_ids_to_symlink = {dataset_symlink.to_dataset_id for dataset_symlink in dataset_symlinks}
    dataset_ids = {initial_dataset.id} | dataset_ids_from_symlink | dataset_ids_to_symlink
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    # Threat all datasets from symlinks like they were passed as `start_node_id`
    inputs = [input for input in lineage.inputs if input.dataset_id in dataset_ids]
    input_stats = relation_stats_by_runs(inputs)
    assert inputs

    outputs = [output for output in lineage.outputs if output.dataset_id in dataset_ids]
    output_stats = relation_stats_by_runs(outputs)
    assert outputs

    operation_ids = {input.operation_id for input in inputs} | {output.operation_id for output in outputs}
    operations = [operation for operation in lineage.operations if operation.id in operation_ids]
    assert operations

    run_ids = {operation.run_id for operation in operations}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    datasets = await enrich_datasets(datasets, async_session)
    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": initial_dataset.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": run.job_id},
                "to": {"kind": "RUN", "id": str(run.id)},
            }
            for run in sorted(runs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "SYMLINK",
                "from": {"kind": "DATASET", "id": symlink.from_dataset_id},
                "to": {"kind": "DATASET", "id": symlink.to_dataset_id},
                "type": symlink.type.value,
            }
            for symlink in sorted(dataset_symlinks, key=lambda x: (x.from_dataset_id, x.to_dataset_id))
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "RUN", "id": str(input.run_id)},
                "num_bytes": input_stats[(input.run_id, input.dataset_id)]["num_bytes"],
                "num_rows": input_stats[(input.run_id, input.dataset_id)]["num_rows"],
                "num_files": input_stats[(input.run_id, input.dataset_id)]["num_files"],
                "schema": None,
                "last_interaction_at": input_stats[(input.run_id, input.dataset_id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for input in sorted(inputs, key=lambda x: (x.dataset_id, x.run_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(output.run_id)},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": output.type,
                "num_bytes": output_stats[(output.run_id, output.dataset_id)]["num_bytes"],
                "num_rows": output_stats[(output.run_id, output.dataset_id)]["num_rows"],
                "num_files": output_stats[(output.run_id, output.dataset_id)]["num_files"],
                "schema": None,
                "last_interaction_at": output_stats[(output.run_id, output.dataset_id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for output in sorted(outputs, key=lambda x: (x.run_id, x.dataset_id))
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "id": job.location.id,
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
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
                    "id": dataset.location.id,
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                    "external_id": dataset.location.external_id,
                },
            }
            for dataset in sorted(datasets, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "RUN",
                "id": str(run.id),
                "job_id": run.job_id,
                "created_at": run.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "parent_run_id": str(run.parent_run_id),
                "status": run.status.name,
                "external_id": run.external_id,
                "attempt": run.attempt,
                "persistent_log_url": run.persistent_log_url,
                "running_log_url": run.running_log_url,
                "started_at": run.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "started_by_user": {"name": run.started_by_user.name},
                "start_reason": run.start_reason.value,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run.end_reason,
            }
            for run in runs
        ],
    }
