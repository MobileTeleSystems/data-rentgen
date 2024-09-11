from datetime import timedelta
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import (
    Dataset,
    DatasetSymlink,
    Interaction,
    InteractionType,
    Job,
    Operation,
    Run,
)
from tests.test_server.utils.enrich import enrich_datasets, enrich_jobs, enrich_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]

LINEAGE_FIXTURE_ANNOTATION = tuple[list[Job], list[Run], list[Operation], list[Dataset], list[Interaction]]


async def test_get_job_lineage(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_same_job: LINEAGE_FIXTURE_ANNOTATION,
):
    jobs, runs, operations, datasets, _ = lineage_with_same_job

    [job] = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": runs[0].created_at.isoformat(),
            "point_kind": "JOB",
            "point_id": job.id,
            "direction": "FROM",
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
                "kind": "PARENT",
                "from": {"kind": "RUN", "id": str(operation.run_id)},
                "to": {"kind": "OPERATION", "id": str(operation.id)},
                "type": None,
            }
            for operation in sorted(operations, key=lambda x: x.id)
        ]
        + [
            {
                "kind": "INTERACTION",
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


async def test_get_job_lineage_with_direction_and_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage: LINEAGE_FIXTURE_ANNOTATION,
):
    all_jobs, all_runs, all_operations, all_datasets, all_interactions = lineage

    # There is no guarantee that first job will have any interactions with READ type,
    # so we need to search for any job
    some_interaction = next(interaction for interaction in all_interactions if interaction.type == InteractionType.READ)
    some_operation = next(operation for operation in all_operations if operation.id == some_interaction.operation_id)
    some_run = next(run for run in all_runs if run.id == some_operation.run_id)
    job = next(job for job in all_jobs if job.id == some_run.job_id)

    since = min(run.created_at for run in all_runs if run.job_id == job.id)
    until = since + timedelta(seconds=1)

    runs = [run for run in all_runs if run.job_id == job.id and since <= run.created_at <= until]
    run_ids = {run.id for run in runs}
    assert runs

    operations = [
        operation
        for operation in all_operations
        if operation.run_id in run_ids and since <= operation.created_at <= until
    ]
    operation_ids = {operation.id for operation in operations}
    assert operations

    interactions = [
        interaction
        for interaction in all_interactions
        if interaction.type == InteractionType.READ
        and interaction.operation_id in operation_ids
        and since <= interaction.created_at <= until
    ]
    assert interactions

    dataset_ids = {interaction.dataset_id for interaction in interactions}
    datasets = [dataset for dataset in all_datasets if dataset.id in dataset_ids]

    [job] = await enrich_jobs([job], async_session)
    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/lineage",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "point_kind": "JOB",
            "point_id": job.id,
            "direction": "TO",
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
                "kind": "INTERACTION",
                "from": {"kind": "DATASET", "id": interaction.dataset_id},
                "to": {"kind": "OPERATION", "id": str(interaction.operation_id)},
                "type": "READ",
            }
            for interaction in interactions
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
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


async def test_get_job_lineage_with_depth(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LINEAGE_FIXTURE_ANNOTATION,
):
    all_jobs, all_runs, all_operations, all_datasets, all_interactions = lineage_with_depth

    # There is no guarantee that first job will have any interactions with APPEND type,
    # so we need to search for any job
    some_interaction = next(
        interaction for interaction in all_interactions if interaction.type == InteractionType.APPEND
    )
    some_operation = next(operation for operation in all_operations if operation.id == some_interaction.operation_id)
    some_run = next(run for run in all_runs if run.id == some_operation.run_id)
    some_job = next(job for job in all_jobs if job.id == some_run.job_id)

    # Go job -> runs -> operations
    first_level_runs = [run for run in all_runs if run.job_id == some_job.id]
    first_level_run_ids = {run.id for run in first_level_runs}
    first_level_operations = [operation for operation in all_operations if operation.run_id in first_level_run_ids]
    first_level_operation_ids = {operation.id for operation in first_level_operations}
    assert first_level_operations

    # Go operations[first level] -> datasets[second level]
    first_level_interactions = [
        interaction
        for interaction in all_interactions
        if interaction.operation_id in first_level_operation_ids and interaction.type == InteractionType.APPEND
    ]
    first_level_dataset_ids = {interaction.dataset_id for interaction in first_level_interactions}
    first_level_datasets = [dataset for dataset in all_datasets if dataset.id in first_level_dataset_ids]
    assert first_level_datasets

    # Go datasets[second level] -> operations[second level]
    second_level_interactions = [
        interaction
        for interaction in all_interactions
        if interaction.dataset_id in first_level_dataset_ids and interaction.type == InteractionType.READ
    ]
    second_level_operation_ids = {
        interaction.operation_id for interaction in second_level_interactions
    } - first_level_operation_ids
    second_level_operations = [operation for operation in all_operations if operation.id in second_level_operation_ids]
    assert second_level_operations

    # Go operations[second level] -> datasets[third level]
    # There are more levels in this graph, but we stop here
    third_level_interactions = [
        interaction
        for interaction in all_interactions
        if interaction.operation_id in second_level_operation_ids and interaction.type == InteractionType.APPEND
    ]
    third_level_dataset_ids = {
        interaction.dataset_id for interaction in third_level_interactions
    } - first_level_dataset_ids
    third_level_datasets = [dataset for dataset in all_datasets if dataset.id in third_level_dataset_ids]
    assert third_level_datasets

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

    since = min(run.created_at for run in runs)
    response = await test_client.get(
        "v1/lineage",
        params={
            "since": since.isoformat(),
            "point_kind": "JOB",
            "point_id": some_job.id,
            "direction": "FROM",
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
                "kind": "INTERACTION",
                "from": {"kind": "OPERATION", "id": str(interaction.operation_id)},
                "to": {"kind": "DATASET", "id": interaction.dataset_id},
                "type": "APPEND",
            }
            for interaction in first_level_interactions
        ]
        + [
            {
                "kind": "INTERACTION",
                "from": {"kind": "DATASET", "id": interaction.dataset_id},
                "to": {"kind": "OPERATION", "id": str(interaction.operation_id)},
                "type": "READ",
            }
            for interaction in second_level_interactions
        ]
        + [
            {
                "kind": "INTERACTION",
                "from": {"kind": "OPERATION", "id": str(interaction.operation_id)},
                "to": {"kind": "DATASET", "id": interaction.dataset_id},
                "type": "APPEND",
            }
            for interaction in third_level_interactions
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
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


async def test_get_job_lineage_with_depth_ignore_cycles(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_same_job: LINEAGE_FIXTURE_ANNOTATION,
):
    [job], runs, operations, all_datasets, all_interactions = lineage_with_same_job

    # Go operations[first level] -> datasets[second level]
    first_level_interactions = [
        interaction for interaction in all_interactions if interaction.type == InteractionType.APPEND
    ]
    first_level_dataset_ids = {interaction.dataset_id for interaction in first_level_interactions}
    first_level_datasets = [dataset for dataset in all_datasets if dataset.id in first_level_dataset_ids]
    first_level_operation_ids = {interaction.operation_id for interaction in first_level_interactions}
    assert first_level_datasets

    # The there is a cycle dataset[second level] -> operation[first level], so there is only one level of lineage.
    second_level_interactions = [
        interaction
        for interaction in all_interactions
        if interaction.dataset_id in first_level_dataset_ids and interaction.type == InteractionType.READ
    ]
    second_level_operation_ids = {
        interaction.operation_id for interaction in second_level_interactions
    } - first_level_operation_ids
    assert not second_level_operation_ids

    [job] = await enrich_jobs([job], async_session)
    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(first_level_datasets, async_session)

    since = min(run.created_at for run in runs)
    response = await test_client.get(
        "v1/lineage",
        params={
            "since": since.isoformat(),
            "point_kind": "JOB",
            "point_id": job.id,
            "direction": "FROM",
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
                "kind": "INTERACTION",
                "from": {"kind": "OPERATION", "id": str(interaction.operation_id)},
                "to": {"kind": "DATASET", "id": interaction.dataset_id},
                "type": "APPEND",
            }
            for interaction in first_level_interactions
        ]
        + [
            {
                "kind": "INTERACTION",
                "from": {"kind": "DATASET", "id": interaction.dataset_id},
                "to": {"kind": "OPERATION", "id": str(interaction.operation_id)},
                "type": "READ",
            }
            for interaction in second_level_interactions
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
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


async def test_get_job_lineage_with_symlinks(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_symlinks: tuple[
        list[Job],
        list[Run],
        list[Operation],
        list[Dataset],
        list[DatasetSymlink],
        list[Interaction],
    ],
):
    all_jobs, all_runs, all_operations, all_datasets, all_dataset_symlinks, all_interactions = lineage_with_symlinks

    some_interaction = next(
        interaction for interaction in all_interactions if interaction.type == InteractionType.APPEND
    )
    some_operation = next(operation for operation in all_operations if operation.id == some_interaction.operation_id)
    some_run = next(run for run in all_runs if run.id == some_operation.run_id)
    job = next(job for job in all_jobs if job.id == some_run.job_id)

    runs = [run for run in all_runs if run.job_id == job.id]
    run_ids = {run.id for run in runs}
    assert runs

    operations = [operation for operation in all_operations if operation.run_id in run_ids]
    operation_ids = {operation.id for operation in operations}
    assert operations

    interactions = [
        interaction
        for interaction in all_interactions
        if interaction.operation_id in operation_ids and interaction.type == InteractionType.APPEND
    ]
    dataset_ids = {interaction.dataset_id for interaction in interactions}

    # Dataset from symlinks appear only as SYMLINK location, but not as INTERACTION, because of depth=1
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
    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    since = min(run.created_at for run in runs)
    response = await test_client.get(
        "v1/lineage",
        params={
            "since": since.isoformat(),
            "point_kind": "JOB",
            "point_id": job.id,
            "direction": "FROM",
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
                "kind": "SYMLINK",
                "from": {"kind": "DATASET", "id": symlink.from_dataset_id},
                "to": {"kind": "DATASET", "id": symlink.to_dataset_id},
                "type": symlink.type.value,
            }
            for symlink in sorted(dataset_symlinks, key=lambda x: (x.from_dataset_id, x.to_dataset_id))
        ]
        + [
            {
                "kind": "INTERACTION",
                "from": {"kind": "OPERATION", "id": str(interaction.operation_id)},
                "to": {"kind": "DATASET", "id": interaction.dataset_id},
                "type": "APPEND",
            }
            for interaction in interactions
        ],
        "nodes": [
            {
                "kind": "JOB",
                "id": job.id,
                "name": job.name,
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
