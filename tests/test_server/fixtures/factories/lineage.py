from collections.abc import AsyncGenerator
from typing import AsyncContextManager, Callable

import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import (
    Dataset,
    Interaction,
    InteractionType,
    Job,
    Operation,
    Run,
)
from tests.test_server.fixtures.factories.interaction import interaction_factory

LINEAGE_FIXTURE_ANNOTATION = tuple[list[Job], list[Run], list[Operation], list[Dataset], list[Interaction]]


@pytest_asyncio.fixture()
async def lineage(
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    jobs: list[Job],
    runs: list[Run],
    operations: list[Operation],
    datasets: list[Dataset],
) -> AsyncGenerator[LINEAGE_FIXTURE_ANNOTATION, None]:
    interactions = []
    # each operation interacted with some dataset, both read and append
    for operation, dataset in zip(operations, datasets):
        interactions.append(
            interaction_factory(
                created_at=operation.created_at,
                operation_id=operation.id,
                dataset_id=dataset.id,
                type=InteractionType.READ,
            ),
        )
        interactions.append(
            interaction_factory(
                created_at=operation.created_at,
                operation_id=operation.id,
                dataset_id=dataset.id,
                type=InteractionType.APPEND,
            ),
        )

    # operations are randomly distributed along runs. select only those which will appear in lineage graph
    run_ids = {operation.run_id for operation in operations}
    actual_runs = [run for run in runs if run.id in run_ids]

    # same for jobs
    job_ids = {run.job_id for run in actual_runs}
    actual_jobs = [job for job in jobs if job.id in job_ids]

    async with async_session_maker() as async_session:
        for item in interactions:
            async_session.add(item)
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        for item in interactions:
            await async_session.refresh(item)

        async_session.expunge_all()

    yield actual_jobs, actual_runs, operations, datasets, interactions

    delete_interaction = delete(Interaction).where(
        Interaction.operation_id.in_([operation.id for operation in operations]),
    )
    async with async_session_maker() as async_session:
        await async_session.execute(delete_interaction)
        await async_session.commit()


@pytest_asyncio.fixture()
async def lineage_with_same_job(
    lineage: LINEAGE_FIXTURE_ANNOTATION,
) -> LINEAGE_FIXTURE_ANNOTATION:
    # Select some job, and filter all lineage graph to contain entities somehow related to this job
    [job, *_], all_runs, all_operations, all_datasets, all_interactions = lineage

    runs = [run for run in all_runs if run.job_id == job.id]
    run_ids = {run.id for run in runs}

    operations = [operation for operation in all_operations if operation.run_id in run_ids]
    operation_ids = {operation.id for operation in operations}

    interactions = [interaction for interaction in all_interactions if interaction.operation_id in operation_ids]

    dataset_ids = {interaction.dataset_id for interaction in interactions}
    datasets = [dataset for dataset in all_datasets if dataset.id in dataset_ids]

    return [job], runs, operations, datasets, interactions


@pytest_asyncio.fixture()
async def lineage_with_same_run(
    lineage_with_same_job: LINEAGE_FIXTURE_ANNOTATION,
) -> LINEAGE_FIXTURE_ANNOTATION:
    # Select some run, and filter all lineage graph to contain entities somehow related to this run
    jobs, [run, *_], all_operations, all_datasets, all_interactions = lineage_with_same_job

    operations = [operation for operation in all_operations if operation.run_id == run.id]
    operation_ids = {operation.id for operation in operations}

    interactions = [interaction for interaction in all_interactions if interaction.operation_id in operation_ids]

    dataset_ids = {interaction.dataset_id for interaction in interactions}
    datasets = [dataset for dataset in all_datasets if dataset.id in dataset_ids]

    return jobs, [run], operations, datasets, interactions


@pytest_asyncio.fixture()
async def lineage_with_same_operation(
    lineage_with_same_run: LINEAGE_FIXTURE_ANNOTATION,
) -> LINEAGE_FIXTURE_ANNOTATION:
    # Select some operation, and filter all lineage graph to contain entities somehow related to this operation
    jobs, runs, [operation, *_], all_datasets, all_interactions = lineage_with_same_run

    interactions = [interaction for interaction in all_interactions if interaction.operation_id == operation.id]

    dataset_ids = {interaction.dataset_id for interaction in interactions}
    datasets = [dataset for dataset in all_datasets if dataset.id in dataset_ids]

    return jobs, runs, [operation], datasets, interactions


@pytest_asyncio.fixture()
async def lineage_with_same_dataset(
    lineage: LINEAGE_FIXTURE_ANNOTATION,
) -> LINEAGE_FIXTURE_ANNOTATION:
    # Select some dataset, and filter all lineage graph to contain entities somehow related to this dataset
    all_jobs, all_runs, all_operations, [dataset, *_], all_interactions = lineage

    interactions = [interaction for interaction in all_interactions if interaction.dataset_id == dataset.id]

    operation_ids = {interaction.operation_id for interaction in interactions}
    operations = [operation for operation in all_operations if operation.id in operation_ids]

    run_ids = {operation.run_id for operation in operations}
    runs = [run for run in all_runs if run.id in run_ids]

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in all_jobs if job.id in job_ids]

    return jobs, runs, operations, [dataset], interactions
