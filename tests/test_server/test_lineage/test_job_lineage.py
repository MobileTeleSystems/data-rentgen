from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import (
    Dataset,
    DatasetSymlink,
    Input,
    Job,
    Operation,
    Output,
    Run,
)
from tests.test_server.utils.enrich import enrich_datasets, enrich_jobs, enrich_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio, pytest.mark.lineage]

LINEAGE_FIXTURE_ANNOTATION = tuple[list[Job], list[Run], list[Operation], list[Dataset], list[Input], list[Output]]


@pytest.mark.parametrize("direction", ["DOWNSTREAM", "UPSTREAM", "BOTH"])
async def test_get_job_lineage_unknown_id(
    test_client: AsyncClient,
    new_job: Job,
    direction: str,
):
    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": datetime.now(tz=timezone.utc).isoformat(),
            "start_node_id": new_job.id,
            "direction": direction,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [],
        "nodes": [],
    }


@pytest.mark.parametrize("direction", ["DOWNSTREAM", "UPSTREAM", "BOTH"])
async def test_get_job_lineage_no_runs(
    test_client: AsyncClient,
    async_session: AsyncSession,
    job: Job,
    direction: str,
):
    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": datetime.now(tz=timezone.utc).isoformat(),
            "start_node_id": job.id,
            "direction": direction,
        },
    )

    [job] = await enrich_jobs([job], async_session)

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        # no runs, nothing to show in relations
        "relations": [],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "type": job.location.type,
                    "name": job.location.name,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            },
        ],
    }


@pytest.mark.parametrize("direction", ["DOWNSTREAM", "UPSTREAM", "BOTH"])
async def test_get_job_lineage_no_operations(
    test_client: AsyncClient,
    async_session: AsyncSession,
    job: Job,
    run: Run,
    direction: str,
):
    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": job.id,
            "direction": direction,
        },
    )

    [run] = await enrich_runs([run], async_session)
    [job] = await enrich_jobs([job], async_session)

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        # runs without operations are excluded,
        # but job is left intact
        "relations": [],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "type": job.location.type,
                    "name": job.location.name,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            },
        ],
    }


@pytest.mark.parametrize("direction", ["DOWNSTREAM", "UPSTREAM", "BOTH"])
async def test_get_job_lineage_no_inputs_outputs(
    test_client: AsyncClient,
    async_session: AsyncSession,
    job: Job,
    run: Run,
    operation: Operation,
    direction: str,
):
    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": job.id,
            "direction": direction,
        },
    )

    [run] = await enrich_runs([run], async_session)
    [job] = await enrich_jobs([job], async_session)

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        # runs & operations without inputs/outputs are excluded,
        # but job is left intact
        "relations": [],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "type": job.location.type,
                    "name": job.location.name,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            },
        ],
    }


async def test_get_job_lineage(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_same_job: LINEAGE_FIXTURE_ANNOTATION,
):
    jobs, runs, _, datasets, *_ = lineage_with_same_job

    [job] = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": runs[0].created_at.isoformat(),
            "start_node_id": job.id,
            "direction": "DOWNSTREAM",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "OUTPUT",
                "from": {"kind": "JOB", "id": job.id},
                "to": {"kind": "DATASET", "id": dataset.id},
                "type": "APPEND",
            }
            for dataset in sorted(datasets, key=lambda x: x.id)
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "type": job.location.type,
                    "name": job.location.name,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            },
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
                    "external_id": dataset.location.external_id,
                },
            }
            for dataset in sorted(datasets, key=lambda x: x.id)
        ],
    }


async def test_get_job_lineage_with_run_granularity(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_same_job: LINEAGE_FIXTURE_ANNOTATION,
):
    jobs, runs, _, all_datasets, _, all_outputs = lineage_with_same_job

    [job] = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    run_ids = {run.id for run in runs}
    outputs = [output for output in all_outputs if output.run_id in run_ids]
    dataset_ids = {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in all_datasets if dataset.id in dataset_ids]
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": runs[0].created_at.isoformat(),
            "start_node_id": job.id,
            "granularity": "RUN",
            "direction": "DOWNSTREAM",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": job.id},
                "to": {"kind": "RUN", "id": str(run.id)},
                "type": None,
            }
            for run in sorted(runs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(output.run_id)},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": "APPEND",
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
                    "type": job.location.type,
                    "name": job.location.name,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            },
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


async def test_get_job_lineage_direction_both(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage: LINEAGE_FIXTURE_ANNOTATION,
):
    all_jobs, all_runs, _, all_datasets, all_inputs, all_outputs = lineage

    job = all_jobs[0]

    inputs = [input for input in all_inputs if input.job_id == job.id]
    input_dataset_ids = {input.dataset_id for input in inputs}
    outputs = [output for output in all_outputs if output.job_id == job.id]
    output_dataset_ids = {output.dataset_id for output in outputs}

    datasets = [dataset for dataset in all_datasets if dataset.id in input_dataset_ids | output_dataset_ids]
    datasets = await enrich_datasets(datasets, async_session)
    [job] = await enrich_jobs([job], async_session)

    since = min(run.created_at for run in all_runs if run.job_id == job.id)
    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
            "direction": "BOTH",
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
                    "type": job.location.type,
                    "name": job.location.name,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            },
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
                    "external_id": dataset.location.external_id,
                },
            }
            for dataset in sorted(datasets, key=lambda x: x.id)
        ],
    }


async def test_get_job_lineage_with_direction_and_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage: LINEAGE_FIXTURE_ANNOTATION,
):
    all_jobs, all_runs, _, all_datasets, all_inputs, _ = lineage

    job = all_jobs[0]

    since = min(run.created_at for run in all_runs if run.job_id == job.id)
    until = since + timedelta(seconds=1)

    raw_runs = [run for run in all_runs if run.job_id == job.id and since <= run.created_at <= until]
    run_ids = {run.id for run in raw_runs}
    assert raw_runs

    inputs = [input for input in all_inputs if input.run_id in run_ids and since <= input.created_at <= until]
    assert inputs

    dataset_ids = {input.dataset_id for input in inputs}
    datasets = [dataset for dataset in all_datasets if dataset.id in dataset_ids]

    [job] = await enrich_jobs([job], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "start_node_id": job.id,
            "direction": "UPSTREAM",
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
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "type": job.location.type,
                    "name": job.location.name,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            },
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
                    "external_id": dataset.location.external_id,
                },
            }
            for dataset in sorted(datasets, key=lambda x: x.id)
        ],
    }


async def test_get_job_lineage_with_direction_and_until_and_run_granularity(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage: LINEAGE_FIXTURE_ANNOTATION,
):
    all_jobs, all_runs, all_operations, all_datasets, all_inputs, _ = lineage

    job = all_jobs[0]

    since = min(run.created_at for run in all_runs if run.job_id == job.id)
    until = since + timedelta(seconds=1)

    raw_runs = [run for run in all_runs if run.job_id == job.id and since <= run.created_at <= until]
    run_ids = {run.id for run in raw_runs}
    assert raw_runs

    raw_operations = [
        operation
        for operation in all_operations
        if operation.run_id in run_ids and since <= operation.created_at <= until
    ]
    operation_ids = {operation.id for operation in raw_operations}
    assert raw_operations

    inputs = [
        input for input in all_inputs if input.operation_id in operation_ids and since <= input.created_at <= until
    ]
    assert inputs

    # Only operations with some inputs are returned
    operation_ids = {input.operation_id for input in inputs}
    operations = [operation for operation in all_operations if operation.id in operation_ids]
    assert operations

    # Same for runs
    run_ids = {operation.run_id for operation in operations}
    runs = [run for run in all_runs if run.id in run_ids]
    assert runs

    dataset_ids = {input.dataset_id for input in inputs}
    datasets = [dataset for dataset in all_datasets if dataset.id in dataset_ids]

    [job] = await enrich_jobs([job], async_session)
    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "start_node_id": job.id,
            "granularity": "RUN",
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
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "type": job.location.type,
                    "name": job.location.name,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            },
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


async def test_get_job_lineage_with_depth(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LINEAGE_FIXTURE_ANNOTATION,
):
    all_jobs, all_runs, _, all_datasets, all_inputs, all_outputs = lineage_with_depth

    job = all_jobs[0]

    # Go job[first level] -> datasets[second level]
    first_level_outputs = [output for output in all_outputs if output.job_id == job.id]
    first_level_dataset_ids = {output.dataset_id for output in first_level_outputs}
    first_level_datasets = [dataset for dataset in all_datasets if dataset.id in first_level_dataset_ids]
    assert first_level_datasets

    # Go datasets[second level] -> jobs[second level]
    second_level_inputs = [input for input in all_inputs if input.dataset_id in first_level_dataset_ids]
    second_level_job_ids = {input.job_id for input in second_level_inputs} - {job.id}
    second_level_jobs = [job for job in all_jobs if job.id in second_level_job_ids]
    assert second_level_jobs

    # Go jobs[second level] -> datasets[third level]
    # There are more levels in this graph, but we stop here
    third_level_outputs = [output for output in all_outputs if output.job_id in second_level_job_ids]
    third_level_dataset_ids = {output.dataset_id for output in third_level_outputs} - first_level_dataset_ids
    third_level_datasets = [dataset for dataset in all_datasets if dataset.id in third_level_dataset_ids]
    assert third_level_datasets

    inputs = second_level_inputs
    outputs = first_level_outputs + third_level_outputs

    dataset_ids = first_level_dataset_ids | third_level_dataset_ids
    datasets = [dataset for dataset in all_datasets if dataset.id in dataset_ids]

    jobs = [job] + second_level_jobs

    jobs = await enrich_jobs(jobs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    since = min(run.created_at for run in all_runs)
    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
            "direction": "DOWNSTREAM",
            "depth": 3,
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
            for input in sorted(inputs, key=lambda x: (x.dataset_id, x.job_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "JOB", "id": output.job_id},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": "APPEND",
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
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                    "external_id": dataset.location.external_id,
                },
            }
            for dataset in sorted(datasets, key=lambda x: x.id)
        ],
    }


async def test_get_job_lineage_with_depth_and_run_granularity(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LINEAGE_FIXTURE_ANNOTATION,
):
    all_jobs, all_runs, all_operations, all_datasets, all_inputs, all_outputs = lineage_with_depth

    job = all_jobs[0]

    # Go output[job] -> operations[first level] -> runs[first level]
    first_level_outputs = [output for output in all_outputs if output.job_id == job.id]
    first_level_operation_ids = {output.operation_id for output in first_level_outputs}
    first_level_operations = [operation for operation in all_operations if operation.id in first_level_operation_ids]
    assert first_level_operations
    first_level_run_ids = {operation.run_id for operation in first_level_operations}
    first_level_dataset_ids = {output.dataset_id for output in first_level_outputs}
    first_level_datasets = [dataset for dataset in all_datasets if dataset.id in first_level_dataset_ids]
    assert first_level_datasets

    # Go datasets[second level] -> operations[second level] -> runs[second level] -> jobs[second level]
    second_level_inputs = [input for input in all_inputs if input.dataset_id in first_level_dataset_ids]
    second_level_operation_ids = {input.operation_id for input in second_level_inputs} - first_level_operation_ids
    second_level_operations = [operation for operation in all_operations if operation.id in second_level_operation_ids]
    second_level_run_ids = {operation.run_id for operation in second_level_operations} - first_level_run_ids
    second_level_runs = [run for run in all_runs if run.id in second_level_run_ids]
    second_level_job_ids = {run.job_id for run in second_level_runs} - {job.id}
    second_level_jobs = [job for job in all_jobs if job.id in second_level_job_ids]
    assert second_level_jobs

    # Go runs[second level] -> datasets[third level]
    third_level_outputs = [output for output in all_outputs if output.run_id in second_level_run_ids]
    third_level_dataset_ids = {output.dataset_id for output in third_level_outputs} - first_level_dataset_ids
    third_level_datasets = [dataset for dataset in all_datasets if dataset.id in third_level_dataset_ids]
    assert third_level_datasets

    inputs = second_level_inputs
    outputs = first_level_outputs + third_level_outputs

    dataset_ids = first_level_dataset_ids | third_level_dataset_ids
    datasets = [dataset for dataset in all_datasets if dataset.id in dataset_ids]

    jobs = [job] + second_level_jobs
    run_ids = first_level_run_ids | second_level_run_ids

    runs = [run for run in all_runs if run.id in run_ids]

    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    since = min(run.created_at for run in runs)
    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
            "direction": "DOWNSTREAM",
            "granularity": "RUN",
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
            for input in sorted(inputs, key=lambda x: (x.dataset_id, x.run_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(output.run_id)},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": "APPEND",
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


async def test_get_job_lineage_with_depth_ignore_cycles(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_same_job: LINEAGE_FIXTURE_ANNOTATION,
):
    [job], runs, _, all_datasets, all_inputs, all_outputs = lineage_with_same_job

    # Go operations[first level] -> datasets[second level]
    first_level_outputs = all_outputs
    first_level_dataset_ids = {output.dataset_id for output in first_level_outputs}
    first_level_datasets = [dataset for dataset in all_datasets if dataset.id in first_level_dataset_ids]
    assert first_level_datasets

    # The there is a cycle dataset[second level] -> operation[first level], so there is only one level of lineage.
    second_level_inputs = [input for input in all_inputs if input.dataset_id in first_level_dataset_ids]

    [job] = await enrich_jobs([job], async_session)
    datasets = await enrich_datasets(first_level_datasets, async_session)

    since = min(run.created_at for run in runs)
    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
            "direction": "DOWNSTREAM",
            "depth": 3,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "JOB", "id": job.id},
                "type": None,
            }
            for input in second_level_inputs
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "JOB", "id": job.id},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": "APPEND",
            }
            for output in first_level_outputs
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
                    "external_id": job.location.external_id,
                },
            },
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
                    "external_id": dataset.location.external_id,
                },
            }
            for dataset in sorted(datasets, key=lambda x: x.id)
        ],
    }


async def test_get_job_lineage_with_symlinks(
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
    all_jobs, all_runs, _, all_datasets, all_dataset_symlinks, _, all_outputs = lineage_with_symlinks

    job = all_jobs[0]
    outputs = [output for output in all_outputs if output.job_id == job.id]
    dataset_ids = {output.dataset_id for output in outputs}

    # Dataset from symlinks appear only as SYMLINK location, but not as INPUT, because of depth=1
    dataset_symlinks = [
        dataset_symlink
        for dataset_symlink in all_dataset_symlinks
        if dataset_symlink.from_dataset_id in dataset_ids or dataset_symlink.to_dataset_id in dataset_ids
    ]
    dataset_ids_from_symlink = {dataset_symlink.from_dataset_id for dataset_symlink in dataset_symlinks}
    dataset_ids_to_symlink = {dataset_symlink.to_dataset_id for dataset_symlink in dataset_symlinks}
    dataset_ids = dataset_ids | dataset_ids_from_symlink | dataset_ids_to_symlink
    datasets = [dataset for dataset in all_datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    since = min(run.created_at for run in all_runs)
    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
            "direction": "DOWNSTREAM",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
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
                "kind": "OUTPUT",
                "from": {"kind": "JOB", "id": job.id},
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
                    "external_id": job.location.external_id,
                },
            },
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
                    "external_id": dataset.location.external_id,
                },
            }
            for dataset in sorted(datasets, key=lambda x: x.id)
        ],
    }
