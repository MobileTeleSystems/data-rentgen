from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import (
    datasets_to_json,
    direct_column_lineage_to_json,
    format_datetime,
    indirect_column_lineage_to_json,
    inputs_to_json,
    jobs_to_json,
    operation_parents_to_json,
    operations_to_json,
    outputs_to_json,
    run_parents_to_json,
    runs_to_json,
)
from tests.test_server.utils.enrich import enrich_datasets, enrich_jobs, enrich_runs
from tests.test_server.utils.lineage_result import LineageResult
from tests.test_server.utils.merge import merge_io_by_jobs, merge_io_by_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio, pytest.mark.lineage]


async def test_get_column_lineage_by_operation(
    test_client: AsyncClient,
    async_session: AsyncSession,
    branchy_lineage_with_column_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = branchy_lineage_with_column_lineage
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
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(operation.id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json([run]) + operation_parents_to_json([operation]),
            "symlinks": [],
            "inputs": inputs_to_json(inputs, granularity="OPERATION"),
            "outputs": outputs_to_json(outputs, granularity="OPERATION"),
            "direct_column_lineage": direct_column_lineage_to_json(
                lineage.direct_column_lineage,
                lineage.direct_column_relations,
                "OPERATION",
                operation.id,
            ),
            "indirect_column_lineage": indirect_column_lineage_to_json(
                lineage.indirect_column_lineage,
                lineage.indirect_column_relations,
                "OPERATION",
                operation.id,
            ),
        },
        "nodes": {
            "datasets": datasets_to_json(datasets),
            "jobs": jobs_to_json([job]),
            "runs": runs_to_json([run]),
            "operations": operations_to_json([operation]),
        },
    }


async def test_get_column_lineage_by_run(
    test_client: AsyncClient,
    async_session: AsyncSession,
    branchy_lineage_with_column_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = branchy_lineage_with_column_lineage
    run = lineage.runs[0]
    job = next(job for job in lineage.jobs if job.id == run.job_id)

    inputs = [input for input in lineage.inputs if input.run_id == run.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.run_id == run.id]
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/runs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(run.id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json([run]),
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            "outputs": outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
            "direct_column_lineage": direct_column_lineage_to_json(
                lineage.direct_column_lineage,
                lineage.direct_column_relations,
                "RUN",
                run.id,
            ),
            "indirect_column_lineage": indirect_column_lineage_to_json(
                lineage.indirect_column_lineage,
                lineage.indirect_column_relations,
                "RUN",
                run.id,
            ),
        },
        "nodes": {
            "datasets": datasets_to_json(datasets),
            "jobs": jobs_to_json([job]),
            "runs": runs_to_json([run]),
            "operations": {},
        },
    }


async def test_get_column_lineage_by_job(
    test_client: AsyncClient,
    async_session: AsyncSession,
    branchy_lineage_with_column_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = branchy_lineage_with_column_lineage
    job = lineage.jobs[0]

    inputs = [input for input in lineage.inputs if input.job_id == job.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.job_id == job.id]
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in lineage.runs)

    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
            "outputs": outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
            "direct_column_lineage": direct_column_lineage_to_json(
                lineage.direct_column_lineage,
                lineage.direct_column_relations,
                "JOB",
                job.id,
            ),
            "indirect_column_lineage": indirect_column_lineage_to_json(
                lineage.indirect_column_lineage,
                lineage.indirect_column_relations,
                "JOB",
                job.id,
            ),
        },
        "nodes": {
            "datasets": datasets_to_json(datasets),
            "jobs": jobs_to_json([job]),
            "runs": {},
            "operations": {},
        },
    }


async def test_get_column_lineage_by_operation_with_combined_transformations(
    test_client: AsyncClient,
    async_session: AsyncSession,
    duplicated_lineage_with_column_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = duplicated_lineage_with_column_lineage
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
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(operation.id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json([run]) + operation_parents_to_json([operation]),
            "symlinks": [],
            "inputs": inputs_to_json(inputs, granularity="OPERATION"),
            "outputs": outputs_to_json(outputs, granularity="OPERATION"),
            "direct_column_lineage": [
                {
                    "from": {"id": str(datasets[0].id), "kind": "DATASET"},
                    "to": {"id": str(datasets[1].id), "kind": "DATASET"},
                    "fields": {
                        "direct_target_column": [
                            {
                                "field": "direct_source_column",
                                "last_used_at": format_datetime(lineage.operations[0].created_at),
                                "types": [
                                    "IDENTITY",
                                    "TRANSFORMATION",
                                ],
                            },
                        ],
                    },
                },
            ],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets),
            "jobs": jobs_to_json([job]),
            "runs": runs_to_json([run]),
            "operations": operations_to_json([operation]),
        },
    }


async def test_get_column_lineage_by_run_with_combined_transformations(
    test_client: AsyncClient,
    async_session: AsyncSession,
    duplicated_lineage_with_column_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = duplicated_lineage_with_column_lineage
    run = lineage.runs[0]
    job = next(job for job in lineage.jobs if job.id == run.job_id)

    inputs = [input for input in lineage.inputs if input.run_id == run.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.run_id == run.id]
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/runs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(run.id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json([run]),
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            "outputs": outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
            "direct_column_lineage": [
                {
                    "from": {"id": str(datasets[0].id), "kind": "DATASET"},
                    "to": {"id": str(datasets[1].id), "kind": "DATASET"},
                    "fields": {
                        "direct_target_column": [
                            {
                                "field": "direct_source_column",
                                "last_used_at": format_datetime(lineage.operations[0].created_at),
                                "types": [
                                    "IDENTITY",
                                    "TRANSFORMATION",
                                    "TRANSFORMATION_MASKING",
                                ],
                            },
                        ],
                    },
                },
            ],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets),
            "jobs": jobs_to_json([job]),
            "runs": runs_to_json([run]),
            "operations": {},
        },
    }


async def test_get_column_lineage_by_job_with_combined_transformations(
    test_client: AsyncClient,
    async_session: AsyncSession,
    duplicated_lineage_with_column_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = duplicated_lineage_with_column_lineage
    job = lineage.jobs[0]

    inputs = [input for input in lineage.inputs if input.job_id == job.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.job_id == job.id]
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in lineage.runs)

    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
            "outputs": outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
            "direct_column_lineage": [
                {
                    "from": {"id": str(datasets[0].id), "kind": "DATASET"},
                    "to": {"id": str(datasets[1].id), "kind": "DATASET"},
                    "fields": {
                        "direct_target_column": [
                            {
                                "field": "direct_source_column",
                                "last_used_at": format_datetime(lineage.operations[0].created_at),
                                "types": [
                                    "IDENTITY",
                                    "TRANSFORMATION",
                                    "TRANSFORMATION_MASKING",
                                    "AGGREGATION",
                                ],
                            },
                        ],
                    },
                },
            ],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets),
            "jobs": jobs_to_json([job]),
            "runs": {},
            "operations": {},
        },
    }
