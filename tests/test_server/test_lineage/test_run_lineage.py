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


@pytest.mark.parametrize("direction", ["DOWNSTREAM", "UPSTREAM"])
async def test_get_run_lineage_unknown_id(
    test_client: AsyncClient,
    new_run: Run,
    direction: str,
):
    response = await test_client.get(
        "v1/runs/lineage",
        params={
            "since": datetime.now(tz=timezone.utc).isoformat(),
            "start_node_id": str(new_run.id),
            "direction": direction,
            "granularity": "OPERATION",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [],
        "nodes": [],
    }


@pytest.mark.parametrize("direction", ["DOWNSTREAM", "UPSTREAM"])
async def test_get_run_lineage_no_operations(
    test_client: AsyncClient,
    async_session: AsyncSession,
    job: Job,
    run: Run,
    direction: str,
):
    response = await test_client.get(
        "v1/runs/lineage",
        params={
            "since": datetime.now(tz=timezone.utc).isoformat(),
            "start_node_id": str(run.id),
            "direction": direction,
        },
    )

    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": run.job_id},
                "to": {"kind": "RUN", "id": str(run.id)},
                "type": None,
            },
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
                },
            },
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
        ],
    }


@pytest.mark.parametrize("direction", ["DOWNSTREAM", "UPSTREAM"])
async def test_get_run_lineage_no_inputs_outputs(
    test_client: AsyncClient,
    async_session: AsyncSession,
    job: Job,
    run: Run,
    direction: str,
):
    response = await test_client.get(
        "v1/runs/lineage",
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(run.id),
            "direction": direction,
        },
    )

    [run] = await enrich_runs([run], async_session)
    [job] = await enrich_jobs([job], async_session)

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        # operations without inputs/outputs are excluded,
        # but run is left intact
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": run.job_id},
                "to": {"kind": "RUN", "id": str(run.id)},
                "type": None,
            },
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
                },
            },
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
        ],
    }


async def test_get_run_lineage(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_same_run: LINEAGE_FIXTURE_ANNOTATION,
):
    jobs, runs, _, datasets, *_ = lineage_with_same_run

    jobs = await enrich_jobs(jobs, async_session)
    [run] = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/runs/lineage",
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(run.id),
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
            },
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(run.id)},
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
            },
        ],
    }


async def test_get_run_lineage_with_operation_granularity(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_same_run: LINEAGE_FIXTURE_ANNOTATION,
):
    jobs, runs, operations, datasets, *_ = lineage_with_same_run

    jobs = await enrich_jobs(jobs, async_session)
    [run] = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/runs/lineage",
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(run.id),
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
            },
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
                "kind": "OUTPUT",
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
                "type": job.type,
                "location": {
                    "type": job.location.type,
                    "name": job.location.name,
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
            for operation in sorted(operations, key=lambda x: x.id)
        ],
    }


async def test_get_run_lineage_with_direction_and_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage: LINEAGE_FIXTURE_ANNOTATION,
):
    all_jobs, all_runs, all_operations, all_datasets, all_inputs, all_outputs = lineage

    # There is no guarantee that first run will have any inputs,
    # so we need to search for any run
    some_input = all_inputs[0]
    some_operation = next(operation for operation in all_operations if operation.id == some_input.operation_id)
    run = next(run for run in all_runs if run.id == some_operation.run_id)
    job = next(job for job in all_jobs if job.id == run.job_id)

    since = run.created_at
    until = since + timedelta(seconds=1)

    raw_operations = [
        operation
        for operation in all_operations
        if operation.run_id == run.id and since <= operation.created_at <= until
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

    dataset_ids = {input.dataset_id for input in inputs}
    datasets = [dataset for dataset in all_datasets if dataset.id in dataset_ids]

    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/runs/lineage",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "start_node_id": str(run.id),
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
            },
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
            },
        ],
    }


async def test_get_run_lineage_with_direction_and_until_and_operation_granularity(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage: LINEAGE_FIXTURE_ANNOTATION,
):
    all_jobs, all_runs, all_operations, all_datasets, all_inputs, all_outputs = lineage

    # There is no guarantee that first run will have any inputs,
    # so we need to search for any run
    some_input = all_inputs[0]
    some_operation = next(operation for operation in all_operations if operation.id == some_input.operation_id)
    run = next(run for run in all_runs if run.id == some_operation.run_id)
    job = next(job for job in all_jobs if job.id == run.job_id)

    since = run.created_at
    until = since + timedelta(seconds=1)

    raw_operations = [
        operation
        for operation in all_operations
        if operation.run_id == run.id and since <= operation.created_at <= until
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

    dataset_ids = {input.dataset_id for input in inputs}
    datasets = [dataset for dataset in all_datasets if dataset.id in dataset_ids]

    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/runs/lineage",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "start_node_id": str(run.id),
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
            },
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
            for operation in sorted(operations, key=lambda x: x.id)
        ],
    }


async def test_get_run_lineage_with_depth(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LINEAGE_FIXTURE_ANNOTATION,
):
    all_jobs, all_runs, all_operations, all_datasets, all_inputs, all_outputs = lineage_with_depth

    # There is no guarantee that first run will have any outputs
    # so we need to search for any run
    some_output = all_outputs[0]
    some_operation = next(operation for operation in all_operations if operation.id == some_output.operation_id)
    some_run = next(run for run in all_runs if run.id == some_operation.run_id)

    # Go operations[first level] -> datasets[second level]
    first_level_operations = [operation for operation in all_operations if operation.run_id == some_run.id]
    first_level_operation_ids = {operation.id for operation in first_level_operations}
    first_level_outputs = [output for output in all_outputs if output.operation_id in first_level_operation_ids]
    first_level_dataset_ids = {output.dataset_id for output in first_level_outputs}
    first_level_datasets = [dataset for dataset in all_datasets if dataset.id in first_level_dataset_ids]
    assert first_level_datasets

    # Go datasets[second level] -> operations[second level] -> runs[second level]
    second_level_inputs = [input for input in all_inputs if input.dataset_id in first_level_dataset_ids]
    second_level_operation_ids = {input.operation_id for input in second_level_inputs} - first_level_operation_ids
    second_level_operations = [operation for operation in all_operations if operation.id in second_level_operation_ids]
    second_level_run_ids = {operation.run_id for operation in second_level_operations} - {some_run.id}
    second_level_runs = [run for run in all_runs if run.id in second_level_run_ids]
    assert second_level_runs

    # Go runs[second level] -> datasets[third level]
    # There are more levels in this graph, but we stop here
    third_level_outputs = [output for output in all_outputs if output.run_id in second_level_run_ids]
    third_level_dataset_ids = {output.dataset_id for output in third_level_outputs} - first_level_dataset_ids
    third_level_datasets = [dataset for dataset in all_datasets if dataset.id in third_level_dataset_ids]
    assert third_level_datasets

    inputs = second_level_inputs
    outputs = first_level_outputs + third_level_outputs

    dataset_ids = first_level_dataset_ids | third_level_dataset_ids
    datasets = [dataset for dataset in all_datasets if dataset.id in dataset_ids]

    runs = [some_run] + second_level_runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in all_jobs if job.id in job_ids]

    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/runs/lineage",
        params={
            "since": some_run.created_at.isoformat(),
            "start_node_id": str(some_run.id),
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


async def test_get_run_lineage_with_depth_and_operation_granularity(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LINEAGE_FIXTURE_ANNOTATION,
):
    all_jobs, all_runs, all_operations, all_datasets, all_inputs, all_outputs = lineage_with_depth

    # There is no guarantee that first run will have any outputs
    # so we need to search for any run
    some_output = all_outputs[0]
    some_operation = next(operation for operation in all_operations if operation.id == some_output.operation_id)
    some_run = next(run for run in all_runs if run.id == some_operation.run_id)

    # Go operations[first level] -> datasets[second level]
    first_level_operations = [operation for operation in all_operations if operation.run_id == some_run.id]
    first_level_operation_ids = {operation.id for operation in first_level_operations}
    first_level_outputs = [output for output in all_outputs if output.operation_id in first_level_operation_ids]
    first_level_dataset_ids = {output.dataset_id for output in first_level_outputs}
    first_level_datasets = [dataset for dataset in all_datasets if dataset.id in first_level_dataset_ids]
    assert first_level_datasets

    # Go datasets[second level] -> operations[second level]
    second_level_inputs = [input for input in all_inputs if input.dataset_id in first_level_dataset_ids]
    second_level_operation_ids = {input.operation_id for input in second_level_inputs} - first_level_operation_ids
    second_level_operations = [operation for operation in all_operations if operation.id in second_level_operation_ids]
    assert second_level_operations

    # Go operations[second level] -> datasets[third level]
    # There are more levels in this graph, but we stop here
    third_level_outputs = [output for output in all_outputs if output.operation_id in second_level_operation_ids]
    third_level_dataset_ids = {output.dataset_id for output in third_level_outputs} - first_level_dataset_ids
    third_level_datasets = [dataset for dataset in all_datasets if dataset.id in third_level_dataset_ids]
    assert third_level_datasets

    inputs = second_level_inputs
    outputs = first_level_outputs + third_level_outputs

    dataset_ids = first_level_dataset_ids | third_level_dataset_ids
    datasets = [dataset for dataset in all_datasets if dataset.id in dataset_ids]

    operation_ids = first_level_operation_ids | second_level_operation_ids
    operations = [operation for operation in all_operations if operation.id in operation_ids]

    run_ids = {operation.run_id for operation in operations}
    runs = [run for run in all_runs if run.id in run_ids]

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in all_jobs if job.id in job_ids]

    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/runs/lineage",
        params={
            "since": some_run.created_at.isoformat(),
            "start_node_id": str(some_run.id),
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


async def test_get_run_lineage_with_depth_ignore_cycles(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_same_run: LINEAGE_FIXTURE_ANNOTATION,
):
    [job], [run], operations, all_datasets, all_inputs, all_outputs = lineage_with_same_run

    first_level_outputs = all_outputs
    first_level_dataset_ids = {output.dataset_id for output in first_level_outputs}
    first_level_datasets = [dataset for dataset in all_datasets if dataset.id in first_level_dataset_ids]
    first_level_operation_ids = {output.operation_id for output in first_level_outputs}
    assert first_level_datasets

    # The there is a cycle dataset -> operation, so there is only one level of lineage.
    # All relations should be in the response, without duplicates.
    second_level_inputs = [input for input in all_inputs if input.dataset_id in first_level_dataset_ids]
    second_level_operation_ids = {input.operation_id for input in second_level_inputs} - first_level_operation_ids
    assert not second_level_operation_ids

    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)
    datasets = await enrich_datasets(first_level_datasets, async_session)

    response = await test_client.get(
        "v1/runs/lineage",
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(run.id),
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
            },
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "RUN", "id": str(input.run_id)},
                "type": None,
            }
            for input in sorted(second_level_inputs, key=lambda x: (x.dataset_id, x.run_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(output.run_id)},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": "APPEND",
            }
            for output in sorted(first_level_outputs, key=lambda x: (x.run_id, x.dataset_id))
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
            },
        ],
    }


async def test_get_run_lineage_with_depth_ignore_cycles_with_operation_granularity(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_same_run: LINEAGE_FIXTURE_ANNOTATION,
):
    [job], [run], operations, all_datasets, all_inputs, all_outputs = lineage_with_same_run

    first_level_outputs = all_outputs
    first_level_dataset_ids = {output.dataset_id for output in first_level_outputs}
    first_level_datasets = [dataset for dataset in all_datasets if dataset.id in first_level_dataset_ids]
    first_level_operation_ids = {output.operation_id for output in first_level_outputs}
    assert first_level_datasets

    # The there is a cycle dataset -> operation, so there is only one level of lineage.
    # All relations should be in the response, without duplicates.
    second_level_inputs = [input for input in all_inputs if input.dataset_id in first_level_dataset_ids]
    second_level_operation_ids = {input.operation_id for input in second_level_inputs} - first_level_operation_ids
    assert not second_level_operation_ids

    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)
    datasets = await enrich_datasets(first_level_datasets, async_session)

    response = await test_client.get(
        "v1/runs/lineage",
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(run.id),
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
            },
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
            for input in sorted(second_level_inputs, key=lambda x: (x.dataset_id, x.operation_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "OPERATION", "id": str(output.operation_id)},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": "APPEND",
            }
            for output in sorted(first_level_outputs, key=lambda x: (x.operation_id, x.dataset_id))
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
            for operation in sorted(operations, key=lambda x: x.id)
        ],
    }


async def test_get_run_lineage_with_symlinks(
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
    all_jobs, all_runs, all_operations, all_datasets, all_dataset_symlinks, all_inputs, all_outputs = (
        lineage_with_symlinks
    )

    some_input = all_inputs[0]
    some_operation = next(operation for operation in all_operations if operation.id == some_input.operation_id)
    run = next(run for run in all_runs if run.id == some_operation.run_id)
    job = next(job for job in all_jobs if job.id == run.job_id)

    operations = [operation for operation in all_operations if operation.run_id == run.id]
    operation_ids = {operation.id for operation in operations}
    assert operations

    outputs = [output for output in all_outputs if output.operation_id in operation_ids]
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
    [run] = await enrich_runs([run], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/runs/lineage",
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(run.id),
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
            },
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
            },
        ],
    }


async def test_get_run_lineage_with_symlinks_and_operation_granularity(
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
    all_jobs, all_runs, all_operations, all_datasets, all_dataset_symlinks, all_inputs, all_outputs = (
        lineage_with_symlinks
    )

    some_input = all_inputs[0]
    some_operation = next(operation for operation in all_operations if operation.id == some_input.operation_id)
    run = next(run for run in all_runs if run.id == some_operation.run_id)
    job = next(job for job in all_jobs if job.id == run.job_id)

    operations = [operation for operation in all_operations if operation.run_id == run.id]
    operation_ids = {operation.id for operation in operations}
    assert operations

    outputs = [output for output in all_outputs if output.operation_id in operation_ids]
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
    [run] = await enrich_runs([run], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/runs/lineage",
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(run.id),
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
            },
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
                "from": {"kind": "OPERATION", "id": str(output.operation_id)},
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
            for operation in sorted(operations, key=lambda x: x.id)
        ],
    }
