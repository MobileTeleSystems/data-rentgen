from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Dataset
from data_rentgen.db.models.output import OutputType
from tests.fixtures.mocks import MockedUser
from tests.test_server.fixtures.factories.schema import create_schema
from tests.test_server.utils.convert_to_json import (
    datasets_to_json,
    inputs_to_json,
    jobs_to_json,
    operation_parents_to_json,
    operations_to_json,
    outputs_to_json,
    run_parents_to_json,
    runs_to_json,
    symlinks_to_json,
)
from tests.test_server.utils.enrich import enrich_datasets, enrich_jobs, enrich_runs
from tests.test_server.utils.lineage_result import LineageResult
from tests.test_server.utils.merge import merge_io_by_jobs, merge_io_by_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio, pytest.mark.lineage]


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
        "relations": {
            "parents": [],
            "symlinks": [],
            "inputs": [],
            "outputs": [],
        },
        "nodes": {
            "datasets": datasets_to_json([dataset]),
            "jobs": {},
            "runs": {},
            "operations": {},
        },
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

    inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]
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
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            "outputs": outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
        },
        "nodes": {
            "datasets": datasets_to_json([dataset]),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
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

    inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]
    assert outputs

    job_ids = {input.job_id for input in inputs} | {output.job_id for output in outputs}
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
        "relations": {
            "parents": [],
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
            "outputs": outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
        },
        "nodes": {
            "datasets": datasets_to_json([dataset]),
            "jobs": jobs_to_json(jobs),
            "runs": {},
            "operations": {},
        },
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
        "relations": {
            "parents": run_parents_to_json(runs) + operation_parents_to_json(operations),
            "symlinks": [],
            "inputs": inputs_to_json(inputs, granularity="OPERATION"),
            "outputs": outputs_to_json(outputs, granularity="OPERATION"),
        },
        "nodes": {
            "datasets": datasets_to_json([dataset]),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": operations_to_json(operations),
        },
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

    inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]

    run_ids = {input.run_id for input in inputs}
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
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            "outputs": [],
        },
        "nodes": {
            "datasets": datasets_to_json([dataset]),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
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

    outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]
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
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": [],
            "inputs": [],
            "outputs": outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
        },
        "nodes": {
            "datasets": datasets_to_json([dataset]),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
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

    inputs_with_until = [input for input in inputs if since <= input.created_at <= until]
    assert inputs_with_until

    outputs_with_until = [output for output in outputs if since <= output.created_at <= until]
    assert outputs_with_until

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
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            "outputs": outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
        },
        "nodes": {
            "datasets": datasets_to_json([dataset]),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
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

    inputs = first_level_inputs + second_level_inputs + third_level_inputs
    assert inputs

    outputs = first_level_outputs + second_level_outputs + third_level_outputs
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
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": first_level_dataset.id,
            "depth": 3,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            "outputs": outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
        },
        "nodes": {
            "datasets": datasets_to_json(datasets),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
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

    inputs = first_level_inputs + second_level_inputs + third_level_inputs
    assert inputs

    outputs = first_level_outputs + second_level_outputs + third_level_outputs
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
        "relations": {
            "parents": [],
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
            "outputs": outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
        },
        "nodes": {
            "datasets": datasets_to_json(datasets),
            "jobs": jobs_to_json(jobs),
            "runs": {},
            "operations": {},
        },
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
        "relations": {
            "parents": run_parents_to_json(runs) + operation_parents_to_json(operations),
            "symlinks": [],
            "inputs": inputs_to_json(inputs, granularity="OPERATION"),
            "outputs": outputs_to_json(outputs, granularity="OPERATION"),
        },
        "nodes": {
            "datasets": datasets_to_json(datasets),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": operations_to_json(operations),
        },
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
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_runs(lineage.inputs), granularity="RUN"),
            "outputs": outputs_to_json(merge_io_by_runs(lineage.outputs), granularity="RUN"),
        },
        "nodes": {
            "datasets": datasets_to_json(datasets),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
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

    inputs = [input for input in lineage.inputs if input.dataset_id in dataset_ids]
    assert inputs

    outputs = [output for output in lineage.outputs if output.dataset_id in dataset_ids]
    assert outputs

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
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            "outputs": outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
        },
        "nodes": {
            "datasets": datasets_to_json(datasets),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
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
    inputs = [input for input in lineage.inputs if input.dataset_id in dataset_ids]
    assert inputs

    outputs = [output for output in lineage.outputs if output.dataset_id in dataset_ids]
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
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": initial_dataset.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": symlinks_to_json(dataset_symlinks),
            "inputs": inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            "outputs": outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
        },
        "nodes": {
            "datasets": datasets_to_json(datasets),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
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
    inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]

    run_ids = {input.run_id for input in inputs} | {output.run_id for output in outputs}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    [dataset] = await enrich_datasets([dataset], async_session)

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
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            "outputs": outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
        },
        "nodes": {
            "datasets": datasets_to_json([dataset]),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
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
    inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]

    run_ids = {input.run_id for input in inputs} | {output.run_id for output in outputs}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    [dataset] = await enrich_datasets([dataset], async_session)

    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
        },
    )

    # merge_io_by_runs sums empty bytes, rows and files, producing 0 instead of None.
    # override that
    merged_inputs = merge_io_by_runs(inputs)
    for input in merged_inputs:
        input.num_bytes = None
        input.num_rows = None
        input.num_files = None

    merged_outputs = merge_io_by_runs(outputs)
    for output in merged_outputs:
        output.num_bytes = None
        output.num_rows = None
        output.num_files = None

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": [],
            "inputs": inputs_to_json(merged_inputs, granularity="RUN"),
            "outputs": outputs_to_json(merged_outputs, granularity="RUN"),
        },
        "nodes": {
            "datasets": datasets_to_json([dataset]),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
    }
