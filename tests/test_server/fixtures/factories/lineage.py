from collections.abc import AsyncGenerator
from typing import AsyncContextManager, Callable

import pytest_asyncio
from sqlalchemy import delete
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
from data_rentgen.db.models.output import OutputType
from tests.test_server.fixtures.factories.input import input_factory
from tests.test_server.fixtures.factories.output import output_factory

LINEAGE_FIXTURE_ANNOTATION = tuple[list[Job], list[Run], list[Operation], list[Dataset], list[Input], list[Output]]


@pytest_asyncio.fixture()
async def lineage(
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    jobs: list[Job],
    runs: list[Run],
    operations: list[Operation],
    datasets: list[Dataset],
) -> AsyncGenerator[LINEAGE_FIXTURE_ANNOTATION, None]:
    inputs = []
    outputs = []
    # Make graph like this:
    # Dataset0 -> Operation0 -> Dataset0
    # Dataset1 -> Operation1 -> Dataset1
    # ...

    # operations are randomly distributed along runs. select only those which will appear in lineage graph
    run_ids = {operation.run_id for operation in operations}
    actual_runs = [run for run in runs if run.id in run_ids]
    run_to_job = {run.id: run.job_id for run in actual_runs}

    # same for jobs
    job_ids = {run.job_id for run in actual_runs}
    actual_jobs = [job for job in jobs if job.id in job_ids]

    for operation, dataset in zip(operations, datasets):
        inputs.append(
            input_factory(
                created_at=operation.created_at,
                operation_id=operation.id,
                run_id=operation.run_id,
                job_id=run_to_job[operation.run_id],
                dataset_id=dataset.id,
            ),
        )
        outputs.append(
            output_factory(
                created_at=operation.created_at,
                operation_id=operation.id,
                run_id=operation.run_id,
                job_id=run_to_job[operation.run_id],
                dataset_id=dataset.id,
                type=OutputType.APPEND,
            ),
        )

    async with async_session_maker() as async_session:
        for item in inputs + outputs:
            async_session.add(item)
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        for item in inputs + outputs:
            await async_session.refresh(item)

        async_session.expunge_all()

    yield actual_jobs, actual_runs, operations, datasets, inputs, outputs

    delete_input = delete(Input).where(
        Input.id.in_([input.id for input in inputs]),
    )
    delete_output = delete(Output).where(
        Output.id.in_([output.id for output in outputs]),
    )
    async with async_session_maker() as async_session:
        await async_session.execute(delete_input)
        await async_session.execute(delete_output)
        await async_session.commit()


@pytest_asyncio.fixture()
async def lineage_with_same_job(
    lineage: LINEAGE_FIXTURE_ANNOTATION,
) -> LINEAGE_FIXTURE_ANNOTATION:
    # Select some job, and filter all lineage graph to contain entities somehow related to this job
    [job, *_], all_runs, all_operations, all_datasets, all_inputs, all_outputs = lineage

    runs = [run for run in all_runs if run.job_id == job.id]
    run_ids = {run.id for run in runs}

    operations = [operation for operation in all_operations if operation.run_id in run_ids]
    operation_ids = {operation.id for operation in operations}

    inputs = [input for input in all_inputs if input.operation_id in operation_ids]
    outputs = [output for output in all_outputs if output.operation_id in operation_ids]

    dataset_ids = {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in all_datasets if dataset.id in dataset_ids]

    return [job], runs, operations, datasets, inputs, outputs


@pytest_asyncio.fixture()
async def lineage_with_same_run(
    lineage_with_same_job: LINEAGE_FIXTURE_ANNOTATION,
) -> LINEAGE_FIXTURE_ANNOTATION:
    # Select some run, and filter all lineage graph to contain entities somehow related to this run
    jobs, [run, *_], all_operations, all_datasets, all_inputs, all_outputs = lineage_with_same_job

    operations = [operation for operation in all_operations if operation.run_id == run.id]
    operation_ids = {operation.id for operation in operations}

    inputs = [input for input in all_inputs if input.operation_id in operation_ids]
    outputs = [output for output in all_outputs if output.operation_id in operation_ids]

    dataset_ids = {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in all_datasets if dataset.id in dataset_ids]

    return jobs, [run], operations, datasets, inputs, outputs


@pytest_asyncio.fixture()
async def lineage_with_same_operation(
    lineage_with_same_run: LINEAGE_FIXTURE_ANNOTATION,
) -> LINEAGE_FIXTURE_ANNOTATION:
    # Select some operation, and filter all lineage graph to contain entities somehow related to this operation
    jobs, runs, [operation, *_], all_datasets, all_inputs, all_outputs = lineage_with_same_run

    inputs = [input for input in all_inputs if input.operation_id == operation.id]
    outputs = [output for output in all_outputs if output.operation_id == operation.id]

    dataset_ids = {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in all_datasets if dataset.id in dataset_ids]

    return jobs, runs, [operation], datasets, inputs, outputs


@pytest_asyncio.fixture()
async def lineage_with_same_dataset(
    lineage: LINEAGE_FIXTURE_ANNOTATION,
) -> LINEAGE_FIXTURE_ANNOTATION:
    # Select some dataset, and filter all lineage graph to contain entities somehow related to this dataset
    all_jobs, all_runs, all_operations, [dataset, *_], all_inputs, all_outputs = lineage

    inputs = [input for input in all_inputs if input.dataset_id == dataset.id]
    outputs = [output for output in all_outputs if output.dataset_id == dataset.id]

    operation_ids = {output.operation_id for output in outputs}
    operations = [operation for operation in all_operations if operation.id in operation_ids]

    run_ids = {operation.run_id for operation in operations}
    runs = [run for run in all_runs if run.id in run_ids]

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in all_jobs if job.id in job_ids]

    return jobs, runs, operations, [dataset], inputs, outputs


@pytest_asyncio.fixture()
async def lineage_with_depth(
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    jobs: list[Job],
    runs: list[Run],
    operations: list[Operation],
    datasets: list[Dataset],
) -> AsyncGenerator[LINEAGE_FIXTURE_ANNOTATION, None]:

    # operations are randomly distributed along runs. select only those which will appear in lineage graph
    run_ids = {operation.run_id for operation in operations}
    actual_runs = [run for run in runs if run.id in run_ids]
    run_to_job = {run.id: run.job_id for run in actual_runs}

    # same for jobs
    job_ids = {run.job_id for run in actual_runs}
    actual_jobs = [job for job in jobs if job.id in job_ids]

    inputs = []
    outputs = []
    # Make graph like this:
    # Dataset0 -> Operation0 -> Dataset1 - ... OperationN -> Dataset0
    for operation, dataset in zip(operations, datasets):
        inputs.append(
            input_factory(
                created_at=operation.created_at,
                operation_id=operation.id,
                run_id=operation.run_id,
                job_id=run_to_job[operation.run_id],
                dataset_id=dataset.id,
            ),
        )

    for operation, dataset in zip(operations, datasets[1:] + datasets[:1]):
        outputs.append(
            output_factory(
                created_at=operation.created_at,
                operation_id=operation.id,
                run_id=operation.run_id,
                job_id=run_to_job[operation.run_id],
                dataset_id=dataset.id,
                type=OutputType.APPEND,
            ),
        )

    async with async_session_maker() as async_session:
        for item in inputs + outputs:
            async_session.add(item)
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        for item in inputs + outputs:
            await async_session.refresh(item)

        async_session.expunge_all()

    yield actual_jobs, actual_runs, operations, datasets, inputs, outputs

    delete_input = delete(Input).where(
        Input.id.in_([input.id for input in inputs]),
    )
    delete_output = delete(Output).where(
        Output.id.in_([output.id for output in outputs]),
    )
    async with async_session_maker() as async_session:
        await async_session.execute(delete_input)
        await async_session.execute(delete_output)
        await async_session.commit()


@pytest_asyncio.fixture()
async def lineage_with_symlinks(
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    jobs: list[Job],
    runs: list[Run],
    operations: list[Operation],
    datasets_with_symlinks: tuple[list[Dataset], list[DatasetSymlink]],
) -> AsyncGenerator[
    tuple[list[Job], list[Run], list[Operation], list[Dataset], list[DatasetSymlink], list[Input], list[Output]],
    None,
]:
    datasets, dataset_symlinks = datasets_with_symlinks

    # operations are randomly distributed along runs. select only those which will appear in lineage graph
    run_ids = {operation.run_id for operation in operations}
    actual_runs = [run for run in runs if run.id in run_ids]
    run_to_job = {run.id: run.job_id for run in actual_runs}

    # same for jobs
    job_ids = {run.job_id for run in actual_runs}
    actual_jobs = [job for job in jobs if job.id in job_ids]

    inputs = []
    outputs = []
    # Make graph like this:
    # Dataset0 -> Operation0 -> Dataset0
    # Dataset1 -> Operation1 -> Dataset1
    # ...
    for operation, dataset in zip(operations, datasets):
        inputs.append(
            input_factory(
                created_at=operation.created_at,
                operation_id=operation.id,
                run_id=operation.run_id,
                job_id=run_to_job[operation.run_id],
                dataset_id=dataset.id,
            ),
        )
        outputs.append(
            output_factory(
                created_at=operation.created_at,
                operation_id=operation.id,
                run_id=operation.run_id,
                job_id=run_to_job[operation.run_id],
                dataset_id=dataset.id,
                type=OutputType.APPEND,
            ),
        )

    async with async_session_maker() as async_session:
        for item in inputs + outputs:
            async_session.add(item)
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        for item in inputs + outputs:
            await async_session.refresh(item)

        async_session.expunge_all()

    yield actual_jobs, actual_runs, operations, datasets, dataset_symlinks, inputs, outputs

    delete_input = delete(Input).where(
        Input.id.in_([input.id for input in inputs]),
    )
    delete_output = delete(Output).where(
        Output.id.in_([output.id for output in outputs]),
    )
    async with async_session_maker() as async_session:
        await async_session.execute(delete_input)
        await async_session.execute(delete_output)
        await async_session.commit()


@pytest_asyncio.fixture()
async def lineage_with_empty_relation_stats(
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    jobs: list[Job],
    runs: list[Run],
    operations: list[Operation],
    datasets: list[Dataset],
) -> AsyncGenerator[LINEAGE_FIXTURE_ANNOTATION, None]:
    inputs = []
    outputs = []
    # Make graph like this:
    # Dataset0 -> Operation0 -> Dataset0
    # Dataset1 -> Operation1 -> Dataset1
    # ...

    # operations are randomly distributed along runs. select only those which will appear in lineage graph
    run_ids = {operation.run_id for operation in operations}
    actual_runs = [run for run in runs if run.id in run_ids]
    run_to_job = {run.id: run.job_id for run in actual_runs}

    # same for jobs
    job_ids = {run.job_id for run in actual_runs}
    actual_jobs = [job for job in jobs if job.id in job_ids]

    for operation, dataset in zip(operations, datasets):
        inputs.append(
            input_factory(
                created_at=operation.created_at,
                operation_id=operation.id,
                run_id=operation.run_id,
                job_id=run_to_job[operation.run_id],
                dataset_id=dataset.id,
                num_bytes=None,
                num_rows=None,
                num_files=None,
            ),
        )
        outputs.append(
            output_factory(
                created_at=operation.created_at,
                operation_id=operation.id,
                run_id=operation.run_id,
                job_id=run_to_job[operation.run_id],
                dataset_id=dataset.id,
                type=OutputType.APPEND,
                num_bytes=None,
                num_rows=None,
                num_files=None,
            ),
        )

    async with async_session_maker() as async_session:
        for item in inputs + outputs:
            async_session.add(item)
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        for item in inputs + outputs:
            await async_session.refresh(item)

        async_session.expunge_all()

    yield actual_jobs, actual_runs, operations, datasets, inputs, outputs

    delete_input = delete(Input).where(
        Input.id.in_([input.id for input in inputs]),
    )
    delete_output = delete(Output).where(
        Output.id.in_([output.id for output in outputs]),
    )
    async with async_session_maker() as async_session:
        await async_session.execute(delete_input)
        await async_session.execute(delete_output)
        await async_session.commit()
