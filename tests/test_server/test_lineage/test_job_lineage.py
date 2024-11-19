from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Job, Run
from tests.test_server.utils.enrich import enrich_datasets, enrich_jobs, enrich_runs
from tests.test_server.utils.lineage_result import LineageResult
from tests.test_server.utils.stats import relation_stats_by_jobs, relation_stats_by_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio, pytest.mark.lineage]


async def test_get_job_lineage_unknown_id(
    test_client: AsyncClient,
    new_job: Job,
):
    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": datetime.now(tz=timezone.utc).isoformat(),
            "start_node_id": new_job.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": [],
        "nodes": [],
    }


async def test_get_job_lineage_no_runs(
    test_client: AsyncClient,
    async_session: AsyncSession,
    job: Job,
):
    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": datetime.now(tz=timezone.utc).isoformat(),
            "start_node_id": job.id,
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
                    "id": job.location.id,
                    "type": job.location.type,
                    "name": job.location.name,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            },
        ],
    }


async def test_get_job_lineage_no_operations(
    test_client: AsyncClient,
    async_session: AsyncSession,
    job: Job,
    run: Run,
):
    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": job.id,
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
                    "id": job.location.id,
                    "type": job.location.type,
                    "name": job.location.name,
                    "addresses": [{"url": address.url} for address in job.location.addresses],
                    "external_id": job.location.external_id,
                },
            },
        ],
    }


async def test_get_job_lineage_no_inputs_outputs(
    test_client: AsyncClient,
    async_session: AsyncSession,
    job: Job,
    run: Run,
):
    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": job.id,
        },
    )

    [run] = await enrich_runs([run], async_session)
    [job] = await enrich_jobs([job], async_session)

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        # runs without inputs/outputs are excluded,
        # but job is left intact
        "relations": [],
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
        ],
    }


async def test_get_job_lineage_simple(
    test_client: AsyncClient,
    async_session: AsyncSession,
    simple_lineage: LineageResult,
):
    lineage = simple_lineage
    job = lineage.jobs[0]

    inputs = [input for input in lineage.inputs if input.job_id == job.id]
    input_stats = relation_stats_by_jobs(lineage.inputs)
    assert inputs

    outputs = [output for output in lineage.outputs if output.job_id == job.id]
    output_stats = relation_stats_by_jobs(lineage.outputs)
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in lineage.runs)

    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
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
        ],
    }


async def test_get_job_lineage_with_direction_downstream(
    test_client: AsyncClient,
    async_session: AsyncSession,
    simple_lineage: LineageResult,
):
    lineage = simple_lineage
    job = lineage.jobs[0]

    outputs = [output for output in lineage.outputs if output.job_id == job.id]
    output_stats = relation_stats_by_jobs(lineage.outputs)
    assert outputs

    dataset_ids = {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in lineage.runs if run.job_id == job.id)

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
        ],
    }


async def test_get_job_lineage_with_direction_upstream(
    test_client: AsyncClient,
    async_session: AsyncSession,
    simple_lineage: LineageResult,
):
    lineage = simple_lineage
    job = lineage.jobs[0]

    inputs = [input for input in lineage.inputs if input.job_id == job.id]
    input_stats = relation_stats_by_jobs(lineage.inputs)
    assert inputs

    dataset_ids = {input.dataset_id for input in inputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in lineage.runs if run.job_id == job.id)

    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": since.isoformat(),
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
                "num_bytes": input_stats[(input.job_id, input.dataset_id)]["num_bytes"],
                "num_rows": input_stats[(input.job_id, input.dataset_id)]["num_rows"],
                "num_files": input_stats[(input.job_id, input.dataset_id)]["num_files"],
                "schema": None,
                "last_interaction_at": input_stats[(input.job_id, input.dataset_id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for input in sorted(inputs, key=lambda x: (x.dataset_id, x.job_id))
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
        ],
    }


async def test_get_job_lineage_with_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    three_days_lineage: LineageResult,
):
    lineage = three_days_lineage
    job = lineage.jobs[0]
    since = min(run.created_at for run in lineage.runs if run.job_id == job.id)
    until = since + timedelta(seconds=1)

    inputs = [input for input in lineage.inputs if input.job_id == job.id and since <= input.created_at <= until]
    input_stats = relation_stats_by_jobs(inputs)
    assert inputs

    outputs = [output for output in lineage.outputs if output.job_id == job.id and since <= output.created_at <= until]
    output_stats = relation_stats_by_jobs(outputs)
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "start_node_id": job.id,
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
        ],
    }


async def test_get_job_lineage_with_granularity_run(
    test_client: AsyncClient,
    async_session: AsyncSession,
    simple_lineage: LineageResult,
):
    lineage = simple_lineage
    job = lineage.jobs[0]
    since = min(run.created_at for run in lineage.runs if run.job_id == job.id)

    inputs = [input for input in lineage.inputs if input.job_id == job.id and since <= input.created_at]
    input_stats = relation_stats_by_runs(inputs)

    outputs = [output for output in lineage.outputs if output.job_id == job.id and since <= output.created_at]
    output_stats = relation_stats_by_runs(outputs)

    # Only runs with relations are returned
    run_ids = {input.run_id for input in inputs} | {output.run_id for output in outputs}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]

    [job] = await enrich_jobs([job], async_session)
    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
            "granularity": "RUN",
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
            }
            for run in sorted(runs, key=lambda x: x.id)
        ],
    }


async def test_get_job_lineage_with_depth(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LineageResult,
):
    lineage = lineage_with_depth
    # Select only relations marked with *
    # D1 -*> J1 -*> D2
    # D2 -*> J2 -*> D3
    # D3 --> J3 --> D4

    job = lineage.jobs[0]

    # Go job[first level] -> datasets[second level]
    first_level_inputs = [input for input in lineage.inputs if input.job_id == job.id]
    first_level_outputs = [output for output in lineage.outputs if output.job_id == job.id]
    first_level_input_dataset_ids = {input.dataset_id for input in first_level_inputs}
    first_level_output_dataset_ids = {output.dataset_id for output in first_level_outputs}
    first_level_dataset_ids = first_level_input_dataset_ids | first_level_output_dataset_ids
    assert first_level_dataset_ids

    # Go datasets[second level] -> jobs[second level]
    second_level_inputs = [input for input in lineage.inputs if input.dataset_id in first_level_output_dataset_ids]
    second_level_outputs = [output for output in lineage.outputs if output.dataset_id in first_level_input_dataset_ids]
    second_level_input_job_ids = {input.job_id for input in second_level_inputs}
    second_level_output_job_ids = {output.job_id for output in second_level_outputs}
    second_level_job_ids = second_level_input_job_ids | second_level_output_job_ids - {job.id}
    second_level_jobs = [job for job in lineage.jobs if job.id in second_level_job_ids]
    assert second_level_jobs

    # Go jobs[second level] -> datasets[third level]
    # There are more levels in this graph, but we stop here
    third_level_inputs = [input for input in lineage.inputs if input.job_id in second_level_output_job_ids]
    third_level_outputs = [output for output in lineage.outputs if output.job_id in second_level_input_job_ids]
    third_level_input_dataset_ids = {input.dataset_id for input in third_level_inputs}
    third_level_output_dataset_ids = {output.dataset_id for output in third_level_outputs}
    third_level_dataset_ids = third_level_input_dataset_ids | third_level_output_dataset_ids - first_level_dataset_ids
    assert third_level_dataset_ids

    inputs = first_level_inputs + second_level_inputs + third_level_inputs
    outputs = first_level_outputs + second_level_outputs + third_level_outputs
    input_stats = relation_stats_by_jobs(inputs)
    output_stats = relation_stats_by_jobs(outputs)

    dataset_ids = first_level_dataset_ids | third_level_dataset_ids
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]

    jobs = [job] + second_level_jobs

    jobs = await enrich_jobs(jobs, async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in lineage.runs if run.job_id == job.id)

    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
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
                "num_bytes": input_stats[(input.job_id, input.dataset_id)]["num_bytes"],
                "num_rows": input_stats[(input.job_id, input.dataset_id)]["num_rows"],
                "num_files": input_stats[(input.job_id, input.dataset_id)]["num_files"],
                "schema": None,
                "last_interaction_at": input_stats[(input.job_id, input.dataset_id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for input in sorted(inputs, key=lambda x: (x.dataset_id, x.run_id))
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
        ],
    }


async def test_get_job_lineage_with_depth_and_granularity_run(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LineageResult,
):
    lineage = lineage_with_depth
    # Select only relations marked with *
    # J1 -*> R1, D1 -*> R1 -*> D2
    # J2 -*> R2, D2 -*> R2 -*> D3
    # J3 --> R3, D3 --> R3 --> D4

    first_level_job_id = lineage.jobs[0].id

    # Go job[first level] -> runs[first level] -> datasets[first level]
    first_level_inputs = [input for input in lineage.inputs if input.job_id == first_level_job_id]
    first_level_outputs = [output for output in lineage.outputs if output.job_id == first_level_job_id]
    first_level_input_run_ids = {input.run_id for input in first_level_inputs}
    first_level_output_run_ids = {output.run_id for output in first_level_outputs}
    first_level_run_ids = first_level_input_run_ids | first_level_output_run_ids
    first_level_input_dataset_ids = {input.dataset_id for input in first_level_inputs}
    first_level_output_dataset_ids = {output.dataset_id for output in first_level_outputs}
    first_level_dataset_ids = first_level_input_dataset_ids | first_level_output_dataset_ids
    assert first_level_dataset_ids

    # Go datasets[first level] -> runs[second level] -> jobs[second level]
    second_level_inputs = [input for input in lineage.inputs if input.dataset_id in first_level_output_dataset_ids]
    second_level_outputs = [output for output in lineage.outputs if output.dataset_id in first_level_input_dataset_ids]
    second_level_input_run_ids = {input.run_id for input in second_level_inputs}
    second_level_output_run_ids = {output.run_id for output in second_level_outputs}
    second_level_run_ids = second_level_input_run_ids | second_level_output_run_ids - first_level_run_ids
    assert second_level_run_ids

    # Go runs[second level] -> datasets[third level]
    third_level_inputs = [input for input in lineage.inputs if input.run_id in second_level_output_run_ids]
    third_level_outputs = [output for output in lineage.outputs if output.run_id in second_level_input_run_ids]
    third_level_input_dataset_ids = {input.dataset_id for input in third_level_inputs}
    third_level_output_dataset_ids = {output.dataset_id for output in third_level_outputs}
    third_level_dataset_ids = third_level_input_dataset_ids | third_level_output_dataset_ids - first_level_dataset_ids
    assert third_level_dataset_ids

    inputs = first_level_inputs + second_level_inputs + third_level_inputs
    outputs = first_level_outputs + second_level_outputs + third_level_outputs
    input_stats = relation_stats_by_runs(inputs)
    output_stats = relation_stats_by_runs(outputs)

    dataset_ids = first_level_dataset_ids | third_level_dataset_ids
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]

    run_ids = first_level_run_ids | second_level_run_ids
    runs = [run for run in lineage.runs if run.id in run_ids]

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]

    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": first_level_job_id,
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
            for input in sorted(inputs, key=lambda x: (x.dataset_id, x.job_id))
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


async def test_get_job_lineage_with_depth_ignore_cycles(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth_and_cycle: LineageResult,
):
    lineage = lineage_with_depth_and_cycle
    # Select all relations:
    # J1 -*> R1 -*> O1, D1 -*> O1 -*> D2
    # J2 -*> R2 -*> O2, D2 -*> O2 -*> D1

    # We can start at any job
    job = lineage.jobs[0]

    output_stats = relation_stats_by_jobs(lineage.outputs)
    input_stats = relation_stats_by_jobs(lineage.inputs)

    jobs = await enrich_jobs(lineage.jobs, async_session)
    datasets = await enrich_datasets(lineage.datasets, async_session)
    since = min(run.created_at for run in lineage.runs)

    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
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
                "num_bytes": input_stats[(input.job_id, input.dataset_id)]["num_bytes"],
                "num_rows": input_stats[(input.job_id, input.dataset_id)]["num_rows"],
                "num_files": input_stats[(input.job_id, input.dataset_id)]["num_files"],
                "schema": None,
                "last_interaction_at": input_stats[(input.job_id, input.dataset_id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for input in sorted(lineage.inputs, key=lambda x: (x.dataset_id, x.job_id))
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
            for output in sorted(lineage.outputs, key=lambda x: (x.job_id, x.dataset_id))
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
        ],
    }


async def test_get_job_lineage_with_symlinks(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_symlinks: LineageResult,
):
    lineage = lineage_with_symlinks

    job = lineage.jobs[1]
    inputs = [input for input in lineage.inputs if input.job_id == job.id]
    input_stats = relation_stats_by_jobs(inputs)
    assert inputs

    outputs = [output for output in lineage.outputs if output.job_id == job.id]
    output_stats = relation_stats_by_jobs(outputs)
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    assert dataset_ids

    # Dataset from symlinks appear only as SYMLINK relation, but not as INPUT, because of depth=1
    dataset_symlinks = [
        dataset_symlink
        for dataset_symlink in lineage.dataset_symlinks
        if dataset_symlink.from_dataset_id in dataset_ids or dataset_symlink.to_dataset_id in dataset_ids
    ]
    dataset_ids_from_symlink = {dataset_symlink.from_dataset_id for dataset_symlink in dataset_symlinks}
    dataset_ids_to_symlink = {dataset_symlink.to_dataset_id for dataset_symlink in dataset_symlinks}
    dataset_ids_include_symlinks = dataset_ids | dataset_ids_from_symlink | dataset_ids_to_symlink
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids_include_symlinks]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in lineage.runs if run.job_id == job.id)

    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
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
        ],
    }
