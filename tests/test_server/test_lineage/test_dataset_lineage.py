from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Dataset
from data_rentgen.db.models.output import OutputType
from tests.fixtures.mocks import MockedUser
from tests.test_server.fixtures.factories.schema import create_schema
from tests.test_server.utils.enrich import enrich_datasets, enrich_jobs, enrich_runs
from tests.test_server.utils.lineage_result import LineageResult
from tests.test_server.utils.merge import merge_io_by_jobs, merge_io_by_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio, pytest.mark.lineage]


async def test_get_dataset_lineage_unknown_id(
    test_client: AsyncClient,
    new_dataset: Dataset,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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


async def test_get_dataset_lineage_unauthorized(
    test_client: AsyncClient,
):
    response = await test_client.get("v1/datasets/lineage")

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {"code": "unauthorized", "details": None, "message": "Missing auth credentials"},
    }, response.json()


async def test_get_dataset_lineage_no_relations(
    test_client: AsyncClient,
    async_session: AsyncSession,
    dataset: Dataset,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
    mocked_user: MockedUser,
):
    lineage = three_days_lineage
    # We need a middle dataset, which has inputs and outputs
    dataset = lineage.datasets[1]

    raw_inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    merged_inputs = merge_io_by_runs(raw_inputs)
    assert merged_inputs

    raw_outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]
    merged_outputs = merge_io_by_runs(raw_outputs)
    assert merged_outputs

    run_ids = {input.run_id for input in raw_inputs} | {output.run_id for output in raw_outputs}
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
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                "from": {"kind": "DATASET", "id": merged_input.dataset_id},
                "to": {"kind": "RUN", "id": str(merged_input.run_id)},
                "num_bytes": merged_input.num_bytes,
                "num_rows": merged_input.num_rows,
                "num_files": merged_input.num_files,
                "schema": {
                    "id": merged_input.schema_id,
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in merged_input.schema.fields
                    ],
                },
                "last_interaction_at": merged_input.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_input in sorted(merged_inputs, key=lambda x: (x.dataset_id, x.run_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(merged_output.run_id)},
                "to": {"kind": "DATASET", "id": merged_output.dataset_id},
                "type": merged_output.type,
                "num_bytes": merged_output.num_bytes,
                "num_rows": merged_output.num_rows,
                "num_files": merged_output.num_files,
                "schema": {
                    "id": merged_output.schema_id,
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in merged_output.schema.fields
                    ],
                },
                "last_interaction_at": merged_output.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_output in sorted(merged_outputs, key=lambda x: (x.run_id, x.dataset_id))
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
    mocked_user: MockedUser,
):
    lineage = three_days_lineage
    # We need a middle dataset, which has inputs and outputs
    dataset = lineage.datasets[1]

    raw_inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    merged_inputs = merge_io_by_jobs(raw_inputs)
    assert merged_inputs

    raw_outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]
    merged_outputs = merge_io_by_jobs(raw_outputs)
    assert merged_outputs

    job_ids = {input.job_id for input in raw_inputs} | {output.job_id for output in raw_outputs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    [dataset] = await enrich_datasets([dataset], async_session)
    jobs = await enrich_jobs(jobs, async_session)
    since = min(run.created_at for run in lineage.runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                "from": {"kind": "DATASET", "id": merged_input.dataset_id},
                "to": {"kind": "JOB", "id": merged_input.job_id},
                "num_bytes": merged_input.num_bytes,
                "num_rows": merged_input.num_rows,
                "num_files": merged_input.num_files,
                "schema": {
                    "id": merged_input.schema_id,
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in merged_input.schema.fields
                    ],
                },
                "last_interaction_at": merged_input.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_input in sorted(merged_inputs, key=lambda x: (x.dataset_id, x.job_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "JOB", "id": merged_output.job_id},
                "to": {"kind": "DATASET", "id": merged_output.dataset_id},
                "type": merged_output.type,
                "num_bytes": merged_output.num_bytes,
                "num_rows": merged_output.num_rows,
                "num_files": merged_output.num_files,
                "schema": {
                    "id": merged_output.schema_id,
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in merged_output.schema.fields
                    ],
                },
                "last_interaction_at": merged_output.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_output in sorted(merged_outputs, key=lambda x: (x.job_id, x.dataset_id))
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
    mocked_user: MockedUser,
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
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                    "id": input.schema_id,
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
                    "id": output.schema_id,
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
    mocked_user: MockedUser,
):
    lineage = three_days_lineage
    # We need a middle dataset, which has inputs and outputs
    dataset = lineage.datasets[1]

    raw_inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    merged_inputs = merge_io_by_runs(raw_inputs)
    assert merged_inputs

    run_ids = {input.run_id for input in raw_inputs}
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
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                "from": {"kind": "DATASET", "id": merged_input.dataset_id},
                "to": {"kind": "RUN", "id": str(merged_input.run_id)},
                "num_bytes": merged_input.num_bytes,
                "num_rows": merged_input.num_rows,
                "num_files": merged_input.num_files,
                "schema": {
                    "id": merged_input.schema_id,
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in merged_input.schema.fields
                    ],
                },
                "last_interaction_at": merged_input.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_input in sorted(merged_inputs, key=lambda x: (x.dataset_id, x.run_id))
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
    mocked_user: MockedUser,
):
    lineage = three_days_lineage
    # We need a middle dataset, which has inputs and outputs
    dataset = lineage.datasets[1]

    raw_outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]
    merged_outputs = merge_io_by_runs(raw_outputs)
    assert merged_outputs

    run_ids = {output.run_id for output in raw_outputs}
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
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                "from": {"kind": "RUN", "id": str(merged_output.run_id)},
                "to": {"kind": "DATASET", "id": merged_output.dataset_id},
                "type": merged_output.type,
                "num_bytes": merged_output.num_bytes,
                "num_rows": merged_output.num_rows,
                "num_files": merged_output.num_files,
                "schema": {
                    "id": merged_output.schema_id,
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in merged_output.schema.fields
                    ],
                },
                "last_interaction_at": merged_output.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_output in sorted(merged_outputs, key=lambda x: (x.run_id, x.dataset_id))
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
    mocked_user: MockedUser,
):
    lineage = three_days_lineage
    # We need a middle dataset, which has inputs and outputs
    dataset = lineage.datasets[1]

    inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]

    since = min([input.created_at for input in inputs] + [output.created_at for output in outputs])
    until = since + timedelta(seconds=0.1)

    raw_inputs = [input for input in inputs if since <= input.created_at <= until]
    merged_inputs = merge_io_by_runs(raw_inputs)
    assert merged_inputs

    raw_outputs = [output for output in outputs if since <= output.created_at <= until]
    merged_outputs = merge_io_by_runs(raw_outputs)
    assert merged_outputs

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
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                "from": {"kind": "DATASET", "id": merged_input.dataset_id},
                "to": {"kind": "RUN", "id": str(merged_input.run_id)},
                "num_bytes": merged_input.num_bytes,
                "num_rows": merged_input.num_rows,
                "num_files": merged_input.num_files,
                "schema": {
                    "id": merged_input.schema_id,
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in merged_input.schema.fields
                    ],
                },
                "last_interaction_at": merged_input.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_input in sorted(merged_inputs, key=lambda x: (x.dataset_id, x.run_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(merged_output.run_id)},
                "to": {"kind": "DATASET", "id": merged_output.dataset_id},
                "type": merged_output.type,
                "num_bytes": merged_output.num_bytes,
                "num_rows": merged_output.num_rows,
                "num_files": merged_output.num_files,
                "schema": {
                    "id": merged_output.schema_id,
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in merged_output.schema.fields
                    ],
                },
                "last_interaction_at": merged_output.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_output in sorted(merged_outputs, key=lambda x: (x.run_id, x.dataset_id))
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
    mocked_user: MockedUser,
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

    raw_inputs = first_level_inputs + second_level_inputs + third_level_inputs
    merged_inputs = merge_io_by_runs(raw_inputs)
    assert merged_inputs

    raw_outputs = first_level_outputs + second_level_outputs + third_level_outputs
    merged_outputs = merge_io_by_runs(raw_outputs)
    assert merged_outputs

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
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                "from": {"kind": "DATASET", "id": merged_input.dataset_id},
                "to": {"kind": "RUN", "id": str(merged_input.run_id)},
                "num_bytes": merged_input.num_bytes,
                "num_rows": merged_input.num_rows,
                "num_files": merged_input.num_files,
                "schema": {
                    "id": merged_input.schema_id,
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in merged_input.schema.fields
                    ],
                },
                "last_interaction_at": merged_input.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_input in sorted(merged_inputs, key=lambda x: (x.dataset_id, x.run_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(merged_output.run_id)},
                "to": {"kind": "DATASET", "id": merged_output.dataset_id},
                "type": merged_output.type,
                "num_bytes": merged_output.num_bytes,
                "num_rows": merged_output.num_rows,
                "num_files": merged_output.num_files,
                "schema": {
                    "id": merged_output.schema_id,
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in merged_output.schema.fields
                    ],
                },
                "last_interaction_at": merged_output.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_output in sorted(merged_outputs, key=lambda x: (x.run_id, x.dataset_id))
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
    mocked_user: MockedUser,
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

    raw_inputs = first_level_inputs + second_level_inputs + third_level_inputs
    merged_inputs = merge_io_by_jobs(raw_inputs)
    assert raw_inputs

    raw_outputs = first_level_outputs + second_level_outputs + third_level_outputs
    merged_outputs = merge_io_by_jobs(raw_outputs)
    assert raw_outputs

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
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                "from": {"kind": "DATASET", "id": merged_input.dataset_id},
                "to": {"kind": "JOB", "id": merged_input.job_id},
                "num_bytes": merged_input.num_bytes,
                "num_rows": merged_input.num_rows,
                "num_files": merged_input.num_files,
                "schema": {
                    "id": merged_input.schema_id,
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in merged_input.schema.fields
                    ],
                },
                "last_interaction_at": merged_input.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_input in sorted(merged_inputs, key=lambda x: (x.dataset_id, x.job_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "JOB", "id": merged_output.job_id},
                "to": {"kind": "DATASET", "id": merged_output.dataset_id},
                "type": merged_output.type,
                "num_bytes": merged_output.num_bytes,
                "num_rows": merged_output.num_rows,
                "num_files": merged_output.num_files,
                "schema": {
                    "id": merged_output.schema_id,
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in merged_output.schema.fields
                    ],
                },
                "last_interaction_at": merged_output.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_output in sorted(merged_outputs, key=lambda x: (x.job_id, x.dataset_id))
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
    mocked_user: MockedUser,
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
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                    "id": input.schema_id,
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
                    "id": output.schema_id,
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
    mocked_user: MockedUser,
):
    lineage = cyclic_lineage
    # Select all relations:
    # J1 -*> R1 -*> O1, D1 -*> O1 -*> D2
    # J2 -*> R2 -*> O2, D2 -*> O2 -*> D1

    # We can start at any dataset
    dataset = lineage.datasets[0]

    merged_inputs = merge_io_by_runs(lineage.inputs)
    merged_outputs = merge_io_by_runs(lineage.outputs)

    datasets = await enrich_datasets(lineage.datasets, async_session)
    jobs = await enrich_jobs(lineage.jobs, async_session)
    runs = await enrich_runs(lineage.runs, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                "from": {"kind": "DATASET", "id": merged_input.dataset_id},
                "to": {"kind": "RUN", "id": str(merged_input.run_id)},
                "num_bytes": merged_input.num_bytes,
                "num_rows": merged_input.num_rows,
                "num_files": merged_input.num_files,
                "schema": {
                    "id": merged_input.schema_id,
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in merged_input.schema.fields
                    ],
                },
                "last_interaction_at": merged_input.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_input in sorted(merged_inputs, key=lambda x: (x.dataset_id, x.run_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(merged_output.run_id)},
                "to": {"kind": "DATASET", "id": merged_output.dataset_id},
                "type": merged_output.type,
                "num_bytes": merged_output.num_bytes,
                "num_rows": merged_output.num_rows,
                "num_files": merged_output.num_files,
                "schema": {
                    "id": merged_output.schema_id,
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in merged_output.schema.fields
                    ],
                },
                "last_interaction_at": merged_output.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_output in sorted(merged_outputs, key=lambda x: (x.run_id, x.dataset_id))
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
    mocked_user: MockedUser,
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

    raw_inputs = [input for input in lineage.inputs if input.dataset_id in dataset_ids]
    merged_inputs = merge_io_by_runs(raw_inputs)
    assert merged_inputs

    raw_outputs = [output for output in lineage.outputs if output.dataset_id in dataset_ids]
    merged_outputs = merge_io_by_runs(raw_outputs)
    assert merged_outputs

    jobs = await enrich_jobs(lineage.jobs, async_session)
    runs = await enrich_runs(lineage.runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                "from": {"kind": "DATASET", "id": merged_input.dataset_id},
                "to": {"kind": "RUN", "id": str(merged_input.run_id)},
                "num_bytes": merged_input.num_bytes,
                "num_rows": merged_input.num_rows,
                "num_files": merged_input.num_files,
                "schema": {
                    "id": merged_input.schema_id,
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in merged_input.schema.fields
                    ],
                },
                "last_interaction_at": merged_input.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_input in sorted(merged_inputs, key=lambda x: (x.dataset_id, x.run_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(merged_output.run_id)},
                "to": {"kind": "DATASET", "id": merged_output.dataset_id},
                "type": merged_output.type,
                "num_bytes": merged_output.num_bytes,
                "num_rows": merged_output.num_rows,
                "num_files": merged_output.num_files,
                "schema": {
                    "id": merged_output.schema_id,
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in merged_output.schema.fields
                    ],
                },
                "last_interaction_at": merged_output.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_output in sorted(merged_outputs, key=lambda x: (x.run_id, x.dataset_id))
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
    mocked_user: MockedUser,
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
    raw_inputs = [input for input in lineage.inputs if input.dataset_id in dataset_ids]
    merged_inputs = merge_io_by_runs(raw_inputs)
    assert merged_inputs

    raw_outputs = [output for output in lineage.outputs if output.dataset_id in dataset_ids]
    merged_outputs = merge_io_by_runs(raw_outputs)
    assert merged_outputs

    operation_ids = {input.operation_id for input in raw_inputs} | {output.operation_id for output in raw_inputs}
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
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                "from": {"kind": "DATASET", "id": merged_input.dataset_id},
                "to": {"kind": "RUN", "id": str(merged_input.run_id)},
                "num_bytes": merged_input.num_bytes,
                "num_rows": merged_input.num_rows,
                "num_files": merged_input.num_files,
                "schema": {
                    "id": merged_input.schema_id,
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in merged_input.schema.fields
                    ],
                },
                "last_interaction_at": merged_input.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_input in sorted(merged_inputs, key=lambda x: (x.dataset_id, x.run_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(merged_output.run_id)},
                "to": {"kind": "DATASET", "id": merged_output.dataset_id},
                "type": merged_output.type,
                "num_bytes": merged_output.num_bytes,
                "num_rows": merged_output.num_rows,
                "num_files": merged_output.num_files,
                "schema": {
                    "id": merged_output.schema_id,
                    "fields": [
                        {
                            "description": None,
                            "fields": [],
                            **field,
                        }
                        for field in merged_output.schema.fields
                    ],
                },
                "last_interaction_at": merged_output.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_output in sorted(merged_outputs, key=lambda x: (x.run_id, x.dataset_id))
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


@pytest.mark.parametrize("dataset_index", [0, 1], ids=["output", "input"])
async def test_get_dataset_lineage_unmergeable_schema_and_output_type(
    test_client: AsyncClient,
    async_session: AsyncSession,
    duplicated_lineage: LineageResult,
    dataset_index: int,
    mocked_user: MockedUser,
):
    lineage = duplicated_lineage

    # make every input and output a different schema -> grouping by Run returns None.
    # make every output a different type -> grouping by Run returns None.
    for raw_input in lineage.inputs:
        schema = await create_schema(async_session)
        raw_input.schema_id = schema.id
        raw_input.schema = schema
        await async_session.merge(raw_input)

    output_types = list(OutputType)
    for i, raw_output in enumerate(lineage.outputs):
        schema = await create_schema(async_session)
        raw_output.schema_id = schema.id
        raw_output.schema = schema
        raw_output.type = output_types[i % len(output_types)]
        await async_session.merge(raw_output)

    await async_session.commit()

    # one dataset has only inputs, another only outputs - test both
    dataset = lineage.datasets[dataset_index]

    raw_inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    merged_inputs = merge_io_by_runs(raw_inputs)

    raw_outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]
    merged_outputs = merge_io_by_runs(raw_outputs)

    dataset_ids = {input.dataset_id for input in raw_inputs} | {output.dataset_id for output in raw_outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    run_ids = {input.run_id for input in raw_inputs} | {output.run_id for output in raw_outputs}
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
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                "from": {"kind": "DATASET", "id": merged_input.dataset_id},
                "to": {"kind": "RUN", "id": str(merged_input.run_id)},
                "num_bytes": merged_input.num_bytes,
                "num_rows": merged_input.num_rows,
                "num_files": merged_input.num_files,
                "schema": None,
                "last_interaction_at": merged_input.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_input in sorted(merged_inputs, key=lambda x: (x.dataset_id, x.run_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(merged_output.run_id)},
                "to": {"kind": "DATASET", "id": merged_output.dataset_id},
                "type": None,
                "num_bytes": merged_output.num_bytes,
                "num_rows": merged_output.num_rows,
                "num_files": merged_output.num_files,
                "schema": None,
                "last_interaction_at": merged_output.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_output in sorted(merged_outputs, key=lambda x: (x.run_id, x.dataset_id))
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


@pytest.mark.parametrize("dataset_index", [0, 1], ids=["output", "input"])
async def test_get_dataset_lineage_empty_io_stats_and_schema(
    test_client: AsyncClient,
    async_session: AsyncSession,
    duplicated_lineage: LineageResult,
    dataset_index: int,
    mocked_user: MockedUser,
):
    lineage = duplicated_lineage

    # clear input/output stats and schema.
    for raw_input in lineage.inputs:
        raw_input.schema_id = None
        raw_input.schema = None
        raw_input.num_bytes = None
        raw_input.num_rows = None
        raw_input.num_files = None
        await async_session.merge(raw_input)

    for raw_output in lineage.outputs:
        raw_output.schema_id = None
        raw_output.schema = None
        raw_output.num_bytes = None
        raw_output.num_rows = None
        raw_output.num_files = None
        await async_session.merge(raw_output)

    await async_session.commit()

    # one dataset has only inputs, another only outputs - test both
    dataset = lineage.datasets[dataset_index]

    raw_inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    merged_inputs = merge_io_by_runs(raw_inputs)

    raw_outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]
    merged_outputs = merge_io_by_runs(raw_outputs)

    dataset_ids = {input.dataset_id for input in raw_inputs} | {output.dataset_id for output in raw_outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    run_ids = {input.run_id for input in raw_inputs} | {output.run_id for output in raw_outputs}
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
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                "from": {"kind": "DATASET", "id": merged_input.dataset_id},
                "to": {"kind": "RUN", "id": str(merged_input.run_id)},
                "num_bytes": None,
                "num_rows": None,
                "num_files": None,
                "schema": None,
                "last_interaction_at": merged_input.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_input in sorted(merged_inputs, key=lambda x: (x.dataset_id, x.run_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(merged_output.run_id)},
                "to": {"kind": "DATASET", "id": merged_output.dataset_id},
                "type": merged_output.type,
                "num_bytes": None,
                "num_rows": None,
                "num_files": None,
                "schema": None,
                "last_interaction_at": merged_output.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for merged_output in sorted(merged_outputs, key=lambda x: (x.run_id, x.dataset_id))
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
