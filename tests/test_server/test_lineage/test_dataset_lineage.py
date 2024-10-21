from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Dataset, Input, Job, Operation, Output, Run
from data_rentgen.db.models.dataset_symlink import DatasetSymlink
from tests.test_server.utils.enrich import enrich_datasets, enrich_jobs, enrich_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio, pytest.mark.lineage]

LINEAGE_FIXTURE_ANNOTATION = tuple[list[Job], list[Run], list[Operation], list[Dataset], list[Input], list[Output]]


@pytest.mark.parametrize("direction", ["DOWNSTREAM", "UPSTREAM", "BOTH"])
async def test_get_dataset_lineage_unknown_id(
    test_client: AsyncClient,
    new_dataset: Dataset,
    direction: str,
):
    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": datetime.now(tz=timezone.utc).isoformat(),
            "start_node_id": new_dataset.id,
            "direction": direction,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [],
        "nodes": [],
    }


@pytest.mark.parametrize("direction", ["DOWNSTREAM", "UPSTREAM", "BOTH"])
async def test_get_dataset_lineage_no_relations(
    test_client: AsyncClient,
    async_session: AsyncSession,
    dataset: Dataset,
    direction: str,
):
    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": datetime.now(tz=timezone.utc).isoformat(),
            "start_node_id": dataset.id,
            "direction": direction,
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
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                },
            },
        ],
    }


async def test_get_dataset_lineage_with_granularity_run(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_same_dataset: LINEAGE_FIXTURE_ANNOTATION,
):
    jobs, runs, _, datasets, *_ = lineage_with_same_dataset
    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    [dataset] = await enrich_datasets(datasets, async_session)

    since = min(run.created_at for run in runs)
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
                "type": None,
            }
            for run in sorted(runs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": dataset.id},
                "to": {"kind": "RUN", "id": str(run.id)},
                "type": None,
            }
            for run in runs
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "name": job.location.name,
                    "type": job.location.type,
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
            },
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
            }
            for run in runs
        ],
    }


async def test_get_dataset_lineage_with_granularity_job(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_same_dataset: LINEAGE_FIXTURE_ANNOTATION,
):
    jobs, runs, _, datasets, *_ = lineage_with_same_dataset

    jobs = await enrich_jobs(jobs, async_session)
    [dataset] = await enrich_datasets(datasets, async_session)

    since = min(run.created_at for run in runs)
    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
            "direction": "DOWNSTREAM",
            "granularity": "JOB",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": dataset.id},
                "to": {"kind": "JOB", "id": job.id},
                "type": None,
            }
            for job in jobs
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "name": job.location.name,
                    "type": job.location.type,
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
            },
        ],
    }


async def test_get_dataset_lineage_with_granularity_operation(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_same_dataset: LINEAGE_FIXTURE_ANNOTATION,
):
    jobs, runs, operations, datasets, *_ = lineage_with_same_dataset

    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    [dataset] = await enrich_datasets(datasets, async_session)

    since = min(run.created_at for run in runs)
    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
            "direction": "DOWNSTREAM",
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
                "type": None,
            }
            for run in sorted(runs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(operation.run_id)},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
                "type": None,
            }
            for operation in sorted(operations, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": dataset.id},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
                "type": None,
            }
            for operation in operations
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "name": job.location.name,
                    "type": job.location.type,
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
            },
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
            }
            for run in runs
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


async def test_get_dataset_lineage_with_direction_both(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_same_dataset: LINEAGE_FIXTURE_ANNOTATION,
):
    jobs, runs, _, datasets, *_ = lineage_with_same_dataset
    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    [dataset] = await enrich_datasets(datasets, async_session)

    since = min(run.created_at for run in runs)
    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
            "direction": "BOTH",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": run.job_id},
                "to": {"kind": "RUN", "id": str(run.id)},
                "type": None,
            }
            for run in runs
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": dataset.id},
                "to": {"kind": "RUN", "id": str(run.id)},
                "type": None,
            }
            for run in runs
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(run.id)},
                "to": {"kind": "DATASET", "id": dataset.id},
                "type": "APPEND",
            }
            for run in runs
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
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
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                },
            },
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
            }
            for run in sorted(runs, key=lambda x: x.id)
        ],
    }


async def test_get_dataset_lineage_with_direction_and_until_and_granularity_operation(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_same_dataset: LINEAGE_FIXTURE_ANNOTATION,
):
    jobs, runs, all_operations, datasets, *_ = lineage_with_same_dataset
    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    [dataset] = await enrich_datasets(datasets, async_session)

    since = min(run.created_at for run in runs)
    # took only first two operations
    until = since + timedelta(seconds=1)

    operations = [operation for operation in all_operations if since <= operation.created_at <= until]

    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "start_node_id": dataset.id,
            "granularity": "OPERATION",
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
                "type": None,
            }
            for run in runs
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
                "kind": "OUTPUT",
                "from": {"kind": "OPERATION", "id": str(operation.id)},
                "to": {"kind": "DATASET", "id": dataset.id},
                "type": "APPEND",
            }
            for operation in operations
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
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
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                },
            },
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
            }
            for run in sorted(runs, key=lambda x: x.id)
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
            for operation in sorted(operations, key=lambda x: x.id)
        ],
    }


async def test_get_dataset_lineage_with_depth_and_granularity_run(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LINEAGE_FIXTURE_ANNOTATION,
):
    all_jobs, all_runs, _, all_datasets, all_inputs, all_outputs = lineage_with_depth

    # Go dataset -> runs[first level]
    first_level_dataset = all_datasets[0]
    first_level_inputs = [input for input in all_inputs if input.dataset_id == first_level_dataset.id]
    first_level_run_ids = {output.run_id for output in first_level_inputs}
    first_level_runs = [run for run in all_runs if run.id in first_level_run_ids]
    assert first_level_runs

    # Go runs[first level] -> datasets[second level]
    second_level_outputs = [output for output in all_outputs if output.run_id in first_level_run_ids]
    second_level_dataset_ids = {output.dataset_id for output in second_level_outputs}
    second_level_datasets = [dataset for dataset in all_datasets if dataset.id in second_level_dataset_ids]
    assert second_level_datasets

    second_level_run_ids = {output.run_id for output in second_level_outputs}
    second_level_runs = [run for run in all_runs if run.id in second_level_run_ids]
    assert second_level_runs

    # Go datasets[second level] -> runs[third level]
    # There are more levels in this graph, but we stop here
    third_level_dataset_ids = second_level_dataset_ids - {first_level_dataset.id}
    third_level_inputs = [input for input in all_inputs if input.dataset_id in third_level_dataset_ids]
    third_level_datasets = [dataset for dataset in all_datasets if dataset.id in third_level_dataset_ids]
    assert third_level_datasets

    third_level_run_ids = {input.run_id for input in third_level_inputs} - second_level_run_ids - first_level_run_ids
    third_level_runs = [run for run in all_runs if run.id in third_level_run_ids]
    assert third_level_runs

    inputs = first_level_inputs + third_level_inputs
    outputs = second_level_outputs

    dataset_ids = {first_level_dataset.id} | second_level_dataset_ids | third_level_dataset_ids
    datasets = [dataset for dataset in all_datasets if dataset.id in dataset_ids]

    run_ids = first_level_run_ids | second_level_run_ids | third_level_run_ids
    runs = [run for run in all_runs if run.id in run_ids]

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in all_jobs if job.id in job_ids]

    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    since = min(run.created_at for run in runs)
    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": first_level_dataset.id,
            "direction": "DOWNSTREAM",
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
                "type": None,
            }
            for run in sorted(runs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "RUN", "id": str(input.run_id)},
                "type": None,
            }
            for input in inputs
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(output.run_id)},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": "APPEND",
            }
            for output in outputs
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
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
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                },
            }
            for dataset in sorted(datasets, key=lambda x: x.id)
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
            }
            for run in sorted(runs, key=lambda x: x.id)
        ],
    }


async def test_get_dataset_lineage_with_depth_and_granularity_job(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LINEAGE_FIXTURE_ANNOTATION,
):
    all_jobs, all_runs, _, all_datasets, all_inputs, all_outputs = lineage_with_depth

    some_input = all_inputs[0]
    first_level_inputs = [input for input in all_inputs if input.dataset_id == some_input.dataset_id]
    first_level_dataset_ids = {input.dataset_id for input in first_level_inputs}
    first_level_datasets = [dataset for dataset in all_datasets if dataset.id in first_level_dataset_ids]
    assert first_level_inputs
    # Go inputs[first level] -> jobs[first level]
    first_level_job_ids = {input.job_id for input in first_level_inputs}
    first_level_jobs = [job for job in all_jobs if job.id in first_level_job_ids]

    # Go job[first level] -> outputs[second level]
    second_level_outputs = [output for output in all_outputs if output.job_id in first_level_job_ids]
    second_level_dataset_ids = {output.dataset_id for output in second_level_outputs}
    second_level_datasets = [dataset for dataset in all_datasets if dataset.id in second_level_dataset_ids]
    assert second_level_outputs

    # Go outputs[second level] -> jobs[third level]
    third_level_inputs = [input for input in all_inputs if input.dataset_id in second_level_dataset_ids]
    third_level_job_ids = {input.job_id for input in third_level_inputs} - first_level_job_ids
    third_level_jobs = [job for job in all_jobs if job.id in third_level_job_ids]
    assert third_level_jobs

    outputs = second_level_outputs
    inputs = first_level_inputs + third_level_inputs
    datasets = first_level_datasets + second_level_datasets

    jobs = first_level_jobs + third_level_jobs

    jobs = await enrich_jobs(jobs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    since = min(run.created_at for run in all_runs)
    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": some_input.dataset_id,
            "direction": "DOWNSTREAM",
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
                "type": None,
            }
            for input in inputs
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "JOB", "id": output.job_id},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": "APPEND",
            }
            for output in outputs
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
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
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                },
            }
            for dataset in sorted(datasets, key=lambda x: x.id)
        ],
    }


async def test_get_dataset_lineage_with_depth_and_granularity_operation(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LINEAGE_FIXTURE_ANNOTATION,
):
    all_jobs, all_runs, all_operations, all_datasets, all_inputs, all_outputs = lineage_with_depth

    some_input = all_inputs[0]
    first_level_inputs = [input for input in all_inputs if input.dataset_id == some_input.dataset_id]
    first_level_dataset_ids = {input.dataset_id for input in first_level_inputs}
    first_level_datasets = [dataset for dataset in all_datasets if dataset.id in first_level_dataset_ids]

    # Go inputs[first level] -> operations[first level] + runs[first level] + jobs[first level]
    first_level_operation_ids = {input.operation_id for input in first_level_inputs}
    first_level_operations = [operation for operation in all_operations if operation.id in first_level_operation_ids]
    first_level_run_ids = {operation.run_id for operation in first_level_operations}
    first_level_runs = [run for run in all_runs if run.id in first_level_run_ids]
    first_level_job_ids = {run.job_id for run in first_level_runs}
    first_level_jobs = [job for job in all_jobs if job.id in first_level_job_ids]
    assert first_level_runs

    # Go operations[first level] -> outputs[second level]
    second_level_outputs = [output for output in all_outputs if output.operation_id in first_level_operation_ids]
    second_level_dataset_ids = {output.dataset_id for output in second_level_outputs}
    second_level_datasets = [dataset for dataset in all_datasets if dataset.id in second_level_dataset_ids]
    assert second_level_outputs

    # Go outputs[second level] -> operations[third level] + runs[third level] + jobs[third level]
    third_level_inputs = [input for input in all_inputs if input.dataset_id in second_level_dataset_ids]
    third_level_operation_ids = {input.operation_id for input in third_level_inputs} - first_level_operation_ids
    third_level_operations = [operation for operation in all_operations if operation.id in third_level_operation_ids]
    third_level_run_ids = {operation.run_id for operation in third_level_operations} - first_level_run_ids
    third_level_runs = [run for run in all_runs if run.id in third_level_run_ids]
    third_level_job_ids = {run.job_id for run in third_level_runs} - first_level_job_ids
    third_level_jobs = [job for job in all_jobs if job.id in third_level_job_ids]
    assert third_level_runs

    outputs = second_level_outputs
    inputs = first_level_inputs + third_level_inputs
    datasets = first_level_datasets + second_level_datasets
    operations = first_level_operations + third_level_operations
    runs = first_level_runs + third_level_runs
    jobs = first_level_jobs + third_level_jobs

    datasets = await enrich_datasets(datasets, async_session)
    runs = await enrich_runs(runs, async_session)
    jobs = await enrich_jobs(jobs, async_session)

    since = min(run.created_at for run in runs)
    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": some_input.dataset_id,
            "direction": "DOWNSTREAM",
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
                "type": None,
            }
            for run in sorted(runs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(operation.run_id)},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
                "type": None,
            }
            for operation in sorted(operations, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "OPERATION", "id": str(input.operation_id)},
                "type": None,
            }
            for input in sorted(inputs, key=lambda x: (x.dataset_id, x.operation_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "OPERATION", "id": str(output.operation_id)},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": "APPEND",
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
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
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
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                },
            }
            for dataset in sorted(datasets, key=lambda x: x.id)
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
            }
            for run in sorted(runs, key=lambda x: x.id)
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
            for operation in sorted(operations, key=lambda x: x.id)
        ],
    }


async def test_get_dataset_lineage_with_depth_and_granularity_run_ignore_cycles(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_same_dataset: LINEAGE_FIXTURE_ANNOTATION,
):
    jobs, runs, _, [dataset], *_ = lineage_with_same_dataset

    # The there is a cycle dataset -> operation, so there is only one level of lineage.
    # All relations should be in the response, without duplicates.
    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    [dataset] = await enrich_datasets([dataset], async_session)

    since = min(run.created_at for run in runs)
    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
            "direction": "DOWNSTREAM",
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
                "type": None,
            }
            for run in sorted(runs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": dataset.id},
                "to": {"kind": "RUN", "id": str(run.id)},
                "type": None,
            }
            for run in sorted(runs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(run.id)},
                "to": {"kind": "DATASET", "id": dataset.id},
                "type": "APPEND",
            }
            for run in sorted(runs, key=lambda x: x.id)
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
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
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                },
            },
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
            }
            for run in sorted(runs, key=lambda x: x.id)
        ],
    }


async def test_get_dataset_lineage_with_depth_and_granularity_operation_ignore_cycles(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_same_dataset: LINEAGE_FIXTURE_ANNOTATION,
):
    jobs, runs, operations, [dataset], *_ = lineage_with_same_dataset

    # The there is a cycle dataset -> operation, so there is only one level of lineage.
    # All relations should be in the response, without duplicates.
    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    [dataset] = await enrich_datasets([dataset], async_session)

    since = min(run.created_at for run in runs)
    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
            "direction": "DOWNSTREAM",
            "granularity": "OPERATION",
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
                "type": None,
            }
            for run in sorted(runs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(operation.run_id)},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
                "type": None,
            }
            for operation in sorted(operations, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": dataset.id},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
                "type": None,
            }
            for operation in sorted(operations, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "OPERATION", "id": str(operation.id)},
                "to": {"kind": "DATASET", "id": dataset.id},
                "type": "APPEND",
            }
            for operation in sorted(operations, key=lambda x: x.id)
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "name": job.location.name,
                    "type": job.location.type,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
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
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                },
            },
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
            }
            for run in sorted(runs, key=lambda x: x.id)
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
            for operation in sorted(operations, key=lambda x: x.id)
        ],
    }


async def test_get_dataset_lineage_with_symlink_and_granularity_run(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_symlinks: tuple[
        list[Job],
        list[Run],
        list[Operation],
        list[Dataset],
        list[DatasetSymlink],
        list[Input],
        list[Output],
    ],
):
    all_jobs, all_runs, all_operations, all_datasets, all_dataset_symlinks, all_inputs, _ = lineage_with_symlinks
    initial_dataset = all_datasets[0]

    dataset_symlinks = [
        dataset_symlink
        for dataset_symlink in all_dataset_symlinks
        if dataset_symlink.from_dataset_id == initial_dataset.id or dataset_symlink.to_dataset_id == initial_dataset.id
    ]
    dataset_ids_from_symlink = {dataset_symlink.from_dataset_id for dataset_symlink in dataset_symlinks}
    dataset_ids_to_symlink = {dataset_symlink.to_dataset_id for dataset_symlink in dataset_symlinks}
    dataset_ids = {initial_dataset.id} | dataset_ids_from_symlink | dataset_ids_to_symlink
    datasets = [dataset for dataset in all_datasets if dataset.id in dataset_ids]
    assert datasets

    # Threat all datasets from symlinks like they were passed as `start_node_id`
    inputs = [input for input in all_inputs if input.dataset_id in dataset_ids]
    assert inputs

    operation_ids = {input.operation_id for input in inputs}
    operations = [operation for operation in all_operations if operation.id in operation_ids]
    assert operations

    run_ids = {operation.run_id for operation in operations}
    runs = [run for run in all_runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in all_jobs if job.id in job_ids]
    assert jobs

    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    since = min(run.created_at for run in runs)
    response = await test_client.get(
        "v1/datasets/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": initial_dataset.id,
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
                "type": None,
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
                "type": None,
            }
            for input in inputs
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "name": job.location.name,
                    "type": job.location.type,
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
            for dataset in sorted(datasets, key=lambda x: x.id)
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
            }
            for run in runs
        ],
    }
