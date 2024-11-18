from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Job, Operation, Run
from tests.test_server.utils.enrich import enrich_datasets, enrich_jobs, enrich_runs
from tests.test_server.utils.lineage_result import LineageResult

pytestmark = [pytest.mark.server, pytest.mark.asyncio, pytest.mark.lineage]


async def test_get_operation_lineage_unknown_id(
    test_client: AsyncClient,
    new_operation: Operation,
):
    response = await test_client.get(
        "v1/operations/lineage",
        params={
            "since": datetime.now(tz=timezone.utc).isoformat(),
            "start_node_id": str(new_operation.id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [],
        "nodes": [],
    }


async def test_get_operation_lineage_no_inputs_outputs(
    test_client: AsyncClient,
    async_session: AsyncSession,
    job: Job,
    run: Run,
    operation: Operation,
):
    response = await test_client.get(
        "v1/operations/lineage",
        params={
            "since": datetime.now(tz=timezone.utc).isoformat(),
            "start_node_id": str(operation.id),
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
            },
            {
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(operation.run_id)},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
            },
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "id": job.location.id,
                    "type": job.location.type,
                    "name": job.location.name,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            },
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
            },
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
            },
        ],
    }


async def test_get_operation_lineage_simple(
    test_client: AsyncClient,
    async_session: AsyncSession,
    simple_lineage: LineageResult,
):
    lineage = simple_lineage
    operation = lineage.operations[0]
    run = next(run for run in lineage.runs if run.id == operation.run_id)
    job = next(job for job in lineage.jobs if job.id == run.job_id)
    inputs = [input for input in lineage.inputs if input.operation_id == operation.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.operation_id == operation.id]
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/operations/lineage",
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(operation.id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": run.job_id},
                "to": {"kind": "RUN", "id": str(run.id)},
            },
        ]
        + [
            {
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(operation.run_id)},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
            },
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "OPERATION", "id": str(input.operation_id)},
                "num_bytes": input.num_bytes,
                "num_rows": input.num_rows,
                "num_files": input.num_files,
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
            },
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
            },
        ],
    }


async def test_get_operation_lineage_with_direction_downstream(
    test_client: AsyncClient,
    async_session: AsyncSession,
    simple_lineage: LineageResult,
):
    lineage = simple_lineage
    operation = lineage.operations[0]
    run = next(run for run in lineage.runs if run.id == operation.run_id)
    job = next(job for job in lineage.jobs if job.id == run.job_id)

    outputs = [output for output in lineage.outputs if output.operation_id == operation.id]
    assert outputs

    dataset_ids = {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/operations/lineage",
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(operation.id),
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
            },
        ]
        + [
            {
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(operation.run_id)},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
            },
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
            },
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
            },
        ],
    }


async def test_get_operation_lineage_with_direction_upstream(
    test_client: AsyncClient,
    async_session: AsyncSession,
    simple_lineage: LineageResult,
):
    lineage = simple_lineage
    operation = lineage.operations[0]
    run = next(run for run in lineage.runs if run.id == operation.run_id)
    job = next(job for job in lineage.jobs if job.id == run.job_id)

    inputs = [input for input in lineage.inputs if input.operation_id == operation.id]
    assert inputs

    dataset_ids = {input.dataset_id for input in inputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/operations/lineage",
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(operation.id),
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
            },
        ]
        + [
            {
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(operation.run_id)},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
            },
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "OPERATION", "id": str(input.operation_id)},
                "num_bytes": input.num_bytes,
                "num_rows": input.num_rows,
                "num_files": input.num_files,
                "last_interaction_at": input.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for input in sorted(inputs, key=lambda x: (x.dataset_id, x.operation_id))
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "location": {
                    "id": job.location.id,
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
            },
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
            },
        ],
    }


async def test_get_operation_lineage_with_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    simple_lineage: LineageResult,
):
    # TODO: This test should be change cause `until` for operation has sense only for `depth` > 1
    lineage = simple_lineage
    operation = lineage.operations[0]

    run = next(run for run in lineage.runs if run.id == operation.run_id)
    job = next(job for job in lineage.jobs if job.id == run.job_id)

    # Operations within same run are 1 second apart
    since = operation.created_at
    until = since + timedelta(seconds=1)

    inputs = [
        input for input in lineage.inputs if input.operation_id == operation.id and since <= input.created_at <= until
    ]
    assert inputs

    outputs = [
        output
        for output in lineage.outputs
        if output.operation_id == operation.id and since <= output.created_at <= until
    ]
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/operations/lineage",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "start_node_id": str(operation.id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": run.job_id},
                "to": {"kind": "RUN", "id": str(run.id)},
            },
            {
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(operation.run_id)},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
            },
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "OPERATION", "id": str(input.operation_id)},
                "num_bytes": input.num_bytes,
                "num_rows": input.num_rows,
                "num_files": input.num_files,
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
            },
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
            },
        ],
    }


async def test_get_operation_lineage_with_depth(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LineageResult,
):
    lineage = lineage_with_depth
    # Select only relations marked with *
    # J1 -*> R1 -*> O1, D1 -*> O1 -*> D2
    # J2 -*> R2 -*> O2, D2 -*> O2 -*> D3
    # J3 --> R3 --> O3, D3 --> O3 --> D4

    first_level_operation = lineage.operations[0]

    # Go operations[first level] -> datasets[second level]
    first_level_inputs = [input for input in lineage.inputs if input.operation_id == first_level_operation.id]
    first_level_outputs = [output for output in lineage.outputs if output.operation_id == first_level_operation.id]
    first_level_input_dataset_ids = {input.dataset_id for input in first_level_inputs}
    first_level_output_dataset_ids = {output.dataset_id for output in first_level_outputs}
    first_level_dataset_ids = first_level_input_dataset_ids | first_level_output_dataset_ids
    assert first_level_dataset_ids

    # Go datasets[second level] -> operations[second level]
    second_level_inputs = [input for input in lineage.inputs if input.dataset_id in first_level_output_dataset_ids]
    second_level_outputs = [output for output in lineage.outputs if output.dataset_id in first_level_input_dataset_ids]
    second_level_input_operation_ids = {input.operation_id for input in second_level_inputs}
    second_level_output_operation_ids = {output.operation_id for output in second_level_outputs}
    second_level_operation_ids = second_level_input_operation_ids | second_level_output_operation_ids - {
        first_level_operation.id,
    }
    assert second_level_operation_ids

    # Go operations[second level] -> datasets[third level]
    # There are more levels in this graph, but we stop here
    third_level_inputs = [input for input in lineage.inputs if input.operation_id in second_level_output_operation_ids]
    third_level_outputs = [
        output for output in lineage.outputs if output.operation_id in second_level_input_operation_ids
    ]
    third_level_input_dataset_ids = {input.dataset_id for input in third_level_inputs}
    third_level_output_dataset_ids = {output.dataset_id for output in third_level_outputs}
    third_level_dataset_ids = third_level_input_dataset_ids | third_level_output_dataset_ids - first_level_dataset_ids
    assert third_level_dataset_ids

    inputs = first_level_inputs + second_level_inputs + third_level_inputs
    assert inputs

    outputs = first_level_outputs + second_level_outputs + third_level_outputs
    assert outputs

    dataset_ids = first_level_dataset_ids | third_level_dataset_ids
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    operation_ids = {first_level_operation.id} | second_level_operation_ids
    operations = [operation for operation in lineage.operations if operation.id in operation_ids]
    assert operations

    run_ids = {operation.run_id for operation in operations}
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
        "v1/operations/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": str(first_level_operation.id),
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


async def test_get_operation_lineage_with_depth_ignore_cycles(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth_and_cycle: LineageResult,
):
    lineage = lineage_with_depth_and_cycle
    # Select all relations:
    # J1 -*> R1 -*> O1, D1 -*> O1 -*> D2
    # J2 -*> R2 -*> O2, D2 -*> O2 -*> D1

    # We can start at any operation
    operation = lineage.operations[0]

    runs = await enrich_runs(lineage.runs, async_session)
    jobs = await enrich_jobs(lineage.jobs, async_session)
    datasets = await enrich_datasets(lineage.datasets, async_session)
    since = min(run.created_at for run in lineage.runs)

    response = await test_client.get(
        "v1/operations/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": str(operation.id),
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
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(operation.run_id)},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
            }
            for operation in sorted(lineage.operations, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "OPERATION", "id": str(input.operation_id)},
                "num_bytes": input.num_bytes,
                "num_rows": input.num_rows,
                "num_files": input.num_files,
                "last_interaction_at": input.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for input in sorted(lineage.inputs, key=lambda x: (x.dataset_id, x.operation_id))
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
                "last_interaction_at": output.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for output in sorted(lineage.outputs, key=lambda x: (x.operation_id, x.dataset_id))
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
            for dataset in datasets
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
            for operation in sorted(lineage.operations, key=lambda x: x.id)
        ],
    }


async def test_get_operation_lineage_with_symlinks(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_symlinks: LineageResult,
):
    lineage = lineage_with_symlinks
    operation = lineage.operations[0]

    inputs = [input for input in lineage.inputs if input.operation_id == operation.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.operation_id == operation.id]
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}

    dataset_symlinks = [
        dataset_symlink
        for dataset_symlink in lineage.dataset_symlinks
        if dataset_symlink.from_dataset_id in dataset_ids or dataset_symlink.to_dataset_id in dataset_ids
    ]
    dataset_ids_from_symlink = {dataset_symlink.from_dataset_id for dataset_symlink in dataset_symlinks}
    dataset_ids_to_symlink = {dataset_symlink.to_dataset_id for dataset_symlink in dataset_symlinks}
    dataset_ids_with_symlinks = dataset_ids | dataset_ids_from_symlink | dataset_ids_to_symlink
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids_with_symlinks]
    assert datasets

    run = next(run for run in lineage.runs if run.id == operation.run_id)
    job = next(job for job in lineage.jobs if job.id == run.job_id)

    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/operations/lineage",
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(operation.id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": run.job_id},
                "to": {"kind": "RUN", "id": str(run.id)},
            },
            {
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(operation.run_id)},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
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
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "OPERATION", "id": str(input.operation_id)},
                "num_bytes": input.num_bytes,
                "num_rows": input.num_rows,
                "num_files": input.num_files,
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
            },
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
            },
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
            },
        ],
    }


async def test_get_operation_lineage_with_empty_relation_stats(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_empty_relation_stats: LineageResult,
):
    # TODO: REFACTOR. This test has its own fixture, so the code for obtaining entities can be simplified.
    lineage = lineage_with_empty_relation_stats

    operation = lineage.operations[0]

    inputs = [input for input in lineage.inputs if input.operation_id == operation.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.operation_id == operation.id]
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    run = next(run for run in lineage.runs if run.id == operation.run_id)
    job = next(job for job in lineage.jobs if job.id == run.job_id)

    datasets = await enrich_datasets(datasets, async_session)
    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)

    response = await test_client.get(
        "v1/operations/lineage",
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(operation.id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [
            {
                "kind": "PARENT",
                "from": {"kind": "JOB", "id": run.job_id},
                "to": {"kind": "RUN", "id": str(run.id)},
            },
            {
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(operation.run_id)},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
            },
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "OPERATION", "id": str(input.operation_id)},
                "num_bytes": None,
                "num_rows": None,
                "num_files": None,
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
                "num_bytes": None,
                "num_rows": None,
                "num_files": None,
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
            },
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
            },
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
            },
        ],
    }
