from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Job, Run
from tests.test_server.utils.enrich import enrich_datasets, enrich_jobs, enrich_runs
from tests.test_server.utils.lineage_result import LineageResult
from tests.test_server.utils.stats import relation_stats, relation_stats_by_jobs

pytestmark = [pytest.mark.server, pytest.mark.asyncio, pytest.mark.lineage]


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
                    "id": job.location.id,
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
                    "id": job.location.id,
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


async def test_get_job_lineage(
    test_client: AsyncClient,
    async_session: AsyncSession,
    simple_lineage: LineageResult,
):
    lineage = simple_lineage

    [job] = await enrich_jobs(lineage.jobs, async_session)
    runs = await enrich_runs(lineage.runs, async_session)
    dataset_ids = {output.dataset_id for output in lineage.outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    datasets = await enrich_datasets(datasets, async_session)
    output_stats = relation_stats(lineage.outputs)

    since = min(run.created_at for run in runs)
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
                "from": {"kind": "JOB", "id": job.id},
                "to": {"kind": "DATASET", "id": dataset.id},
                "type": "APPEND",
                "num_bytes": output_stats[dataset.id]["num_bytes"],
                "num_rows": output_stats[dataset.id]["num_rows"],
                "num_files": output_stats[dataset.id]["num_files"],
                "last_interaction_at": output_stats[dataset.id]["created_at"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
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


async def test_get_job_lineage_with_run_granularity(
    test_client: AsyncClient,
    async_session: AsyncSession,
    simple_lineage: LineageResult,
):
    lineage = simple_lineage

    [job] = await enrich_jobs(lineage.jobs, async_session)
    runs = await enrich_runs(lineage.runs, async_session)
    dataset_ids = {output.dataset_id for output in lineage.outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    datasets = await enrich_datasets(datasets, async_session)
    output_stats = relation_stats(lineage.outputs)

    since = min(run.created_at for run in runs)
    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": since.isoformat(),
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
            }
            for run in sorted(runs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(output.run_id)},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": "APPEND",
                "num_bytes": output_stats[output.dataset_id]["num_bytes"],
                "num_rows": output_stats[output.dataset_id]["num_rows"],
                "num_files": output_stats[output.dataset_id]["num_files"],
                "last_interaction_at": output_stats[output.dataset_id]["created_at"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
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


async def test_get_job_lineage_direction_both(
    test_client: AsyncClient,
    async_session: AsyncSession,
    simple_lineage: LineageResult,
):
    lineage = simple_lineage

    [job] = await enrich_jobs(lineage.jobs, async_session)
    runs = await enrich_runs(lineage.runs, async_session)
    datasets = await enrich_datasets(lineage.datasets, async_session)
    output_stats = relation_stats(lineage.outputs)
    input_stats = relation_stats(lineage.inputs)

    since = min(run.created_at for run in runs)
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
                "num_bytes": input_stats[input.dataset_id]["num_bytes"],
                "num_rows": input_stats[input.dataset_id]["num_rows"],
                "num_files": input_stats[input.dataset_id]["num_files"],
                "last_interaction_at": input_stats[input.dataset_id]["created_at"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for input in lineage.inputs
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "JOB", "id": output.job_id},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": "APPEND",
                "num_bytes": output_stats[output.dataset_id]["num_bytes"],
                "num_rows": output_stats[output.dataset_id]["num_rows"],
                "num_files": output_stats[output.dataset_id]["num_files"],
                "last_interaction_at": output_stats[output.dataset_id]["created_at"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for output in lineage.outputs
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


async def test_get_job_lineage_with_direction_and_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    three_days_lineage: LineageResult,
):
    lineage = three_days_lineage

    job = lineage.jobs[0]

    since = min(run.created_at for run in lineage.runs if run.job_id == job.id)
    until = since + timedelta(seconds=1)

    raw_runs = [run for run in lineage.runs if run.job_id == job.id and since <= run.created_at <= until]
    run_ids = {run.id for run in raw_runs}
    assert raw_runs

    inputs = [input for input in lineage.inputs if input.run_id in run_ids and since <= input.created_at <= until]
    assert inputs
    input_stats = relation_stats(inputs)

    dataset_ids = {input.dataset_id for input in inputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]

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
                "num_bytes": input_stats[input.dataset_id]["num_bytes"],
                "num_rows": input_stats[input.dataset_id]["num_rows"],
                "num_files": input_stats[input.dataset_id]["num_files"],
                "last_interaction_at": input_stats[input.dataset_id]["created_at"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
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


async def test_get_job_lineage_with_direction_and_until_and_run_granularity(
    test_client: AsyncClient,
    async_session: AsyncSession,
    three_days_lineage: LineageResult,
):
    lineage = three_days_lineage

    job = lineage.jobs[0]

    since = min(run.created_at for run in lineage.runs if run.job_id == job.id)
    until = since + timedelta(seconds=1)

    raw_runs = [run for run in lineage.runs if run.job_id == job.id and since <= run.created_at <= until]
    run_ids = {run.id for run in raw_runs}
    assert raw_runs

    inputs = [input for input in lineage.inputs if input.run_id in run_ids and since <= input.created_at <= until]
    assert inputs
    input_stats = relation_stats(inputs)

    # Only runs with some inputs are returned
    run_ids = {input.run_id for input in inputs}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    dataset_ids = {input.dataset_id for input in inputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]

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
            }
            for run in sorted(runs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "RUN", "id": str(input.run_id)},
                "num_bytes": input_stats[input.dataset_id]["num_bytes"],
                "num_rows": input_stats[input.dataset_id]["num_rows"],
                "num_files": input_stats[input.dataset_id]["num_files"],
                "last_interaction_at": input_stats[input.dataset_id]["created_at"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
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
    # D1 --> J1 -*> D2
    # D2 -*> J2 -*> D3
    # D3 --> J3 --> D4

    job = lineage.jobs[0]

    # Go job[first level] -> datasets[second level]
    first_level_outputs = [output for output in lineage.outputs if output.job_id == job.id]
    first_level_dataset_ids = {output.dataset_id for output in first_level_outputs}
    assert first_level_dataset_ids

    # Go datasets[second level] -> jobs[second level]
    second_level_inputs = [input for input in lineage.inputs if input.dataset_id in first_level_dataset_ids]
    second_level_job_ids = {input.job_id for input in second_level_inputs} - {job.id}
    second_level_jobs = [job for job in lineage.jobs if job.id in second_level_job_ids]
    assert second_level_jobs

    # Go jobs[second level] -> datasets[third level]
    # There are more levels in this graph, but we stop here
    third_level_outputs = [output for output in lineage.outputs if output.job_id in second_level_job_ids]
    third_level_dataset_ids = {output.dataset_id for output in third_level_outputs} - first_level_dataset_ids
    assert third_level_dataset_ids

    inputs = second_level_inputs
    input_stats = relation_stats(inputs)
    outputs = first_level_outputs + third_level_outputs
    output_stats = relation_stats(outputs)

    dataset_ids = first_level_dataset_ids | third_level_dataset_ids
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]

    jobs = [job] + second_level_jobs

    jobs = await enrich_jobs(jobs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    since = min(run.created_at for run in lineage.runs)
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
                "num_bytes": input_stats[input.dataset_id]["num_bytes"],
                "num_rows": input_stats[input.dataset_id]["num_rows"],
                "num_files": input_stats[input.dataset_id]["num_files"],
                "last_interaction_at": input_stats[input.dataset_id]["created_at"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for input in sorted(inputs, key=lambda x: (x.dataset_id, x.job_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "JOB", "id": output.job_id},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": "APPEND",
                "num_bytes": output_stats[output.dataset_id]["num_bytes"],
                "num_rows": output_stats[output.dataset_id]["num_rows"],
                "num_files": output_stats[output.dataset_id]["num_files"],
                "last_interaction_at": output_stats[output.dataset_id]["created_at"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
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


async def test_get_job_lineage_with_depth_and_run_granularity(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LineageResult,
):
    lineage = lineage_with_depth
    # Select only relations marked with *
    # J1 -*> R1, D1 --> R1 -*> D2
    # J2 -*> R2, D2 -*> R2 -*> D3
    # J3 --> R3, D3 --> R3 --> D4

    first_level_job_id = lineage.jobs[0].id

    # Go job[first level] -> runs[first level] -> datasets[first level]
    first_level_outputs = [output for output in lineage.outputs if output.job_id == first_level_job_id]
    first_level_run_ids = {output.run_id for output in first_level_outputs}
    assert first_level_run_ids
    first_level_dataset_ids = {output.dataset_id for output in first_level_outputs}
    assert first_level_dataset_ids

    # Go datasets[first level] -> runs[second level] -> jobs[second level]
    second_level_inputs = [input for input in lineage.inputs if input.dataset_id in first_level_dataset_ids]
    second_level_run_ids = {input.run_id for input in second_level_inputs} - first_level_run_ids
    assert second_level_run_ids
    second_level_job_ids = {input.job_id for input in second_level_inputs} - {first_level_job_id}
    assert second_level_job_ids

    # Go runs[second level] -> datasets[third level]
    third_level_outputs = [output for output in lineage.outputs if output.run_id in second_level_run_ids]
    third_level_dataset_ids = {output.dataset_id for output in third_level_outputs} - first_level_dataset_ids
    assert third_level_dataset_ids

    inputs = second_level_inputs
    input_stats = relation_stats(inputs)
    outputs = first_level_outputs + third_level_outputs
    output_stats = relation_stats(outputs)

    dataset_ids = first_level_dataset_ids | third_level_dataset_ids
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]

    job_ids = {first_level_job_id} | second_level_job_ids
    jobs = [job for job in lineage.jobs if job.id in job_ids]

    run_ids = first_level_run_ids | second_level_run_ids
    runs = [run for run in lineage.runs if run.id in run_ids]

    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    since = min(run.created_at for run in runs)
    response = await test_client.get(
        "v1/jobs/lineage",
        params={
            "since": since.isoformat(),
            "start_node_id": first_level_job_id,
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
            }
            for run in sorted(runs, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "INPUT",
                "from": {"kind": "DATASET", "id": input.dataset_id},
                "to": {"kind": "RUN", "id": str(input.run_id)},
                "num_bytes": input_stats[input.dataset_id]["num_bytes"],
                "num_rows": input_stats[input.dataset_id]["num_rows"],
                "num_files": input_stats[input.dataset_id]["num_files"],
                "last_interaction_at": input_stats[input.dataset_id]["created_at"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for input in sorted(inputs, key=lambda x: (x.dataset_id, x.run_id))
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "RUN", "id": str(output.run_id)},
                "to": {"kind": "DATASET", "id": output.dataset_id},
                "type": "APPEND",
                "num_bytes": output_stats[output.dataset_id]["num_bytes"],
                "num_rows": output_stats[output.dataset_id]["num_rows"],
                "num_files": output_stats[output.dataset_id]["num_files"],
                "last_interaction_at": output_stats[output.dataset_id]["created_at"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
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


async def test_get_job_lineage_with_depth_ignore_cycles(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth_and_cycle: LineageResult,
):
    lineage = lineage_with_depth_and_cycle
    job = lineage.jobs[0]
    jobs = await enrich_jobs(lineage.jobs, async_session)

    output_stats = relation_stats_by_jobs(lineage.outputs)
    input_stats = relation_stats_by_jobs(lineage.inputs)

    [dataset] = await enrich_datasets(lineage.datasets, async_session)

    since = min(run.created_at for run in lineage.runs)
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
                "from": {"kind": "DATASET", "id": dataset.id},
                "to": {"kind": "JOB", "id": job.id},
                "num_bytes": input_stats[(job.id, dataset.id)]["num_bytes"],
                "num_rows": input_stats[(job.id, dataset.id)]["num_rows"],
                "num_files": input_stats[(job.id, dataset.id)]["num_files"],
                "last_interaction_at": input_stats[(job.id, dataset.id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
            }
            for job in jobs
        ]
        + [
            {
                "kind": "OUTPUT",
                "from": {"kind": "JOB", "id": job.id},
                "to": {"kind": "DATASET", "id": dataset.id},
                "type": "APPEND",
                "num_bytes": output_stats[(job.id, dataset.id)]["num_bytes"],
                "num_rows": output_stats[(job.id, dataset.id)]["num_rows"],
                "num_files": output_stats[(job.id, dataset.id)]["num_files"],
                "last_interaction_at": output_stats[(job.id, dataset.id)]["created_at"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                ),
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


async def test_get_job_lineage_with_symlinks(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_symlinks: LineageResult,
):
    lineage = lineage_with_symlinks

    job = lineage.jobs[1]
    outputs = [output for output in lineage.outputs if output.job_id == job.id]
    output_stats = relation_stats(outputs)
    dataset_ids = {output.dataset_id for output in outputs}

    # Dataset from symlinks appear only as SYMLINK location, but not as INPUT, because of depth=1
    dataset_symlinks = [
        dataset_symlink
        for dataset_symlink in lineage.dataset_symlinks
        if dataset_symlink.from_dataset_id in dataset_ids or dataset_symlink.to_dataset_id in dataset_ids
    ]
    dataset_ids_from_symlink = {dataset_symlink.from_dataset_id for dataset_symlink in dataset_symlinks}
    dataset_ids_to_symlink = {dataset_symlink.to_dataset_id for dataset_symlink in dataset_symlinks}
    dataset_ids = dataset_ids | dataset_ids_from_symlink | dataset_ids_to_symlink
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
                "num_bytes": output_stats[output.dataset_id]["num_bytes"],
                "num_rows": output_stats[output.dataset_id]["num_rows"],
                "num_files": output_stats[output.dataset_id]["num_files"],
                "last_interaction_at": output_stats[output.dataset_id]["created_at"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
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
