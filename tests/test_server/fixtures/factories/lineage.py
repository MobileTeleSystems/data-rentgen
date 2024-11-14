from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
from typing import AsyncContextManager, Callable

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import DatasetSymlinkType, Job, User
from data_rentgen.db.models.output import OutputType
from tests.test_server.fixtures.factories.dataset import create_dataset, make_symlink
from tests.test_server.fixtures.factories.input import create_input
from tests.test_server.fixtures.factories.job import create_job
from tests.test_server.fixtures.factories.location import create_location
from tests.test_server.fixtures.factories.operation import create_operation
from tests.test_server.fixtures.factories.output import create_output
from tests.test_server.fixtures.factories.run import create_run
from tests.test_server.utils.delete import clean_db
from tests.test_server.utils.lineage_result import LineageResult


@pytest_asyncio.fixture()
async def simple_lineage(
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    job: Job,
    user: User,
) -> AsyncGenerator[LineageResult, None]:
    # This fixture generates a simple lineage graph with 1 job, 2 runs, 2 operations for each run, and 4 datasets.
    # The structure is as follows: J --> R; R --> O1; R --> O2; D0 --> O1 --> D1; D2 --> O2 --> D3.

    lineage = LineageResult()
    lineage.jobs.append(job)
    num_runs = 2
    async with async_session_maker() as async_session:
        created_at = datetime.now()
        for n in range(num_runs):
            run = await create_run(
                async_session,
                run_kwargs={
                    "job_id": job.id,
                    "created_at": created_at + timedelta(seconds=0.1 * n),
                    "started_by_user_id": user.id,
                },
            )
            lineage.runs.append(run)

            # Each run has 2 operations
            operations = [
                await create_operation(
                    async_session,
                    operation_kwargs={
                        "run_id": run.id,
                        "created_at": run.created_at + timedelta(seconds=0.2 * i),
                    },
                )
                for i in range(2)
            ]
            lineage.operations.extend(operations)

            dataset_locations = [await create_location(async_session) for _ in range(4)]
            datasets = [await create_dataset(async_session, location_id=location.id) for location in dataset_locations]
            lineage.datasets.extend(datasets)

            inputs = [
                await create_input(
                    async_session,
                    input_kwargs={
                        "created_at": operation.created_at,
                        "operation_id": operation.id,
                        "run_id": run.id,
                        "job_id": job.id,
                        "dataset_id": datasets[2 * i].id,
                    },
                )
                for i, operation in enumerate(operations)
            ]
            lineage.inputs.extend(inputs)

            outputs = [
                await create_output(
                    async_session,
                    output_kwargs={
                        "created_at": operation.created_at,
                        "operation_id": operation.id,
                        "run_id": run.id,
                        "job_id": job.id,
                        "dataset_id": datasets[2 * i + 1].id,
                        "type": OutputType.APPEND,
                    },
                )
                for i, operation in enumerate(operations)
            ]
            lineage.outputs.extend(outputs)

        async_session.expunge_all()

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def three_days_lineage(
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    job: Job,
    user: User,
) -> AsyncGenerator[LineageResult, None]:
    # This fixture creates a lineage similar to real data, representing one job with runs spanning three days.
    # Each run includes two operations interacting with three datasets in the sequence:
    # Dataset0 --> Operation0 --> Dataset1 --> Operation1 --> Dataset2.
    lineage = LineageResult()
    lineage.jobs.append(job)
    created_at = datetime.now()

    async with async_session_maker() as async_session:
        for day in range(3):
            run = await create_run(
                async_session,
                run_kwargs={
                    "job_id": job.id,
                    "created_at": created_at + timedelta(days=day),
                    "started_by_user_id": user.id,
                },
            )
            lineage.runs.append(run)
            operations = [
                await create_operation(
                    async_session,
                    operation_kwargs={
                        "run_id": run.id,
                        "created_at": run.created_at + timedelta(seconds=0.2 * i),
                    },
                )
                for i in range(2)
            ]
            lineage.operations.extend(operations)
            dataset_locations = [await create_location(async_session) for _ in range(3)]
            datasets = [await create_dataset(async_session, location_id=location.id) for location in dataset_locations]
            lineage.datasets.extend(datasets)
            inputs = [
                await create_input(
                    async_session,
                    input_kwargs={
                        "created_at": operation.created_at,
                        "operation_id": operation.id,
                        "run_id": operation.run_id,
                        "job_id": job.id,
                        "dataset_id": datasets[i].id,
                    },
                )
                for i, operation in enumerate(operations)
            ]
            lineage.inputs.extend(inputs)
            outputs = [
                await create_output(
                    async_session,
                    output_kwargs={
                        "created_at": operation.created_at + timedelta(seconds=0.1),
                        "operation_id": operation.id,
                        "run_id": operation.run_id,
                        "job_id": job.id,
                        "dataset_id": datasets[i + 1].id,
                        "type": OutputType.APPEND,
                    },
                )
                for i, operation in enumerate(operations)
            ]
            lineage.outputs.extend(outputs)
            async_session.expunge_all()

        yield lineage

        async with async_session_maker() as async_session:
            await clean_db(async_session)


@pytest_asyncio.fixture()
async def lineage_with_depth(
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    user: User,
):
    # This fixture generates a lineage with three simultaneous trees, structures as: J --> R --> O,
    # connected through Input and Output datasets.
    create_at = datetime.now()
    lineage = LineageResult()
    async with async_session_maker() as async_session:
        dataset_locations = [await create_location(async_session) for _ in range(4)]
        datasets = [await create_dataset(async_session, location_id=location.id) for location in dataset_locations]
        lineage.datasets.extend(datasets)

        # Create a job, run and operation with IO datasets.
        for i in range(3):

            job_location = await create_location(async_session)
            job = await create_job(async_session, location_id=job_location.id)
            lineage.jobs.append(job)

            run = await create_run(
                async_session,
                run_kwargs={
                    "job_id": job.id,
                    "started_by_user_id": user.id,
                    "created_at": create_at + timedelta(seconds=i),
                },
            )
            lineage.runs.append(run)

            operation = await create_operation(
                async_session,
                operation_kwargs={
                    "run_id": run.id,
                    "created_at": run.created_at + timedelta(seconds=0.2),
                },
            )
            lineage.operations.append(operation)

            input = await create_input(
                async_session,
                input_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": run.id,
                    "job_id": job.id,
                    "dataset_id": datasets[i].id,
                },
            )
            lineage.inputs.append(input)

            output = await create_output(
                async_session,
                output_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": run.id,
                    "job_id": job.id,
                    "dataset_id": datasets[i + 1].id,
                    "type": OutputType.APPEND,
                },
            )
            lineage.outputs.append(output)

        yield lineage

        async with async_session_maker() as async_session:
            await clean_db(async_session)


@pytest_asyncio.fixture()
async def lineage_with_depth_and_cycle(
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    user: User,
):
    # This fixture generates a lineage with two trees: J --> R --> O,
    # connected through a single Dataset, forming a cycle.
    created_at = datetime.now()
    lineage = LineageResult()
    async with async_session_maker() as async_session:
        dataset_location = await create_location(async_session)
        dataset = await create_dataset(async_session, location_id=dataset_location.id)
        lineage.datasets.append(dataset)

        jobs_location = [await create_location(async_session) for _ in range(2)]
        jobs = [await create_job(async_session, location_id=location.id) for location in jobs_location]
        lineage.jobs.extend(jobs)

        runs = [
            await create_run(
                async_session,
                run_kwargs={
                    "job_id": job.id,
                    "started_by_user_id": user.id,
                    "created_at": created_at + timedelta(seconds=0.1),
                },
            )
            for job in jobs
        ]
        lineage.runs.extend(runs)

        operations = [
            await create_operation(
                async_session,
                operation_kwargs={
                    "run_id": run.id,
                    "created_at": run.created_at + timedelta(seconds=0.2),
                },
            )
            for run in runs
        ]
        lineage.operations.extend(operations)

        inputs = [
            await create_input(
                async_session,
                input_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": operation.run_id,
                    "job_id": job.id,
                    "dataset_id": dataset.id,
                },
            )
            for job, operation in zip(jobs, operations)
        ]
        lineage.inputs.extend(inputs)

        outputs = [
            await create_output(
                async_session,
                output_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": operation.run_id,
                    "job_id": job.id,
                    "dataset_id": dataset.id,
                    "type": OutputType.APPEND,
                },
            )
            for job, operation in zip(jobs, operations)
        ]
        lineage.outputs.extend(outputs)

        yield lineage

        async with async_session_maker() as async_session:
            await clean_db(async_session)


@pytest_asyncio.fixture()
async def lineage_with_symlinks(
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    user: User,
) -> AsyncGenerator[LineageResult, None]:
    # This fixture generates three simple lineage trees, structured as: J --> R --> O, D0 --> O --> D1S.
    # Each dataset has a symlink dataset, and the trees are connected through these symlink datasets.

    lineage = LineageResult()
    created_at = datetime.now()

    async with async_session_maker() as async_session:
        dataset_locations = [await create_location(async_session, location_kwargs={"type": "hdfs"}) for _ in range(4)]
        datasets = [await create_dataset(async_session, location_id=location.id) for location in dataset_locations]
        lineage.datasets.extend(datasets)

        symlink_locations = [await create_location(async_session, location_kwargs={"type": "hive"}) for _ in range(4)]
        symlink_datasets = [
            await create_dataset(async_session, location_id=location.id) for location in symlink_locations
        ]
        lineage.datasets.extend(symlink_datasets)

        # Make symlinks
        for dataset, symlink_dataset in zip(datasets, symlink_datasets):
            metastore = [await make_symlink(async_session, dataset, symlink_dataset, DatasetSymlinkType.METASTORE)]
            lineage.dataset_symlinks.extend(metastore)

            warehouse = [await make_symlink(async_session, symlink_dataset, dataset, DatasetSymlinkType.WAREHOUSE)]
            lineage.dataset_symlinks.extend(warehouse)

        # Make graphs
        for i in range(3):
            job_location = await create_location(async_session)
            job = await create_job(async_session, location_id=job_location.id)
            lineage.jobs.append(job)

            run = await create_run(
                async_session,
                run_kwargs={
                    "job_id": job.id,
                    "started_by_user_id": user.id,
                    "created_at": created_at + timedelta(seconds=i),
                },
            )
            lineage.runs.append(run)

            operation = await create_operation(
                async_session,
                operation_kwargs={
                    "created_at": run.created_at + timedelta(seconds=0.2),
                    "run_id": run.id,
                },
            )
            lineage.operations.append(operation)

            input = await create_input(
                async_session,
                input_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": operation.run_id,
                    "job_id": job.id,
                    "dataset_id": datasets[i].id,
                },
            )
            lineage.inputs.append(input)
            output = await create_output(
                async_session,
                output_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": operation.run_id,
                    "job_id": job.id,
                    "dataset_id": symlink_datasets[i].id,
                    "type": OutputType.APPEND,
                },
            )
            lineage.outputs.append(output)

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def lineage_with_empty_relation_stats(
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    job: Job,
    user: User,
) -> AsyncGenerator[LineageResult, None]:
    # This fixture generates a simple lineage tree structured as J --> R --> O and D0 --> 0 --> D1,
    # with empty statistics in both Input and Output.
    created_at = datetime.now()
    lineage = LineageResult()
    lineage.jobs.append(job)

    async with async_session_maker() as async_session:
        run = await create_run(
            async_session,
            run_kwargs={
                "job_id": job.id,
                "started_by_user_id": user.id,
                "created_at": created_at + timedelta(seconds=1),
            },
        )
        lineage.runs.append(run)

        operation = await create_operation(
            async_session,
            operation_kwargs={
                "run_id": run.id,
                "created_at": run.created_at + timedelta(seconds=0.1),
            },
        )
        lineage.operations.append(operation)

        datasets_location = [await create_location(async_session) for _ in range(2)]
        datasets = [await create_dataset(async_session, location_id=location.id) for location in datasets_location]
        lineage.datasets.extend(datasets)

        input = await create_input(
            async_session,
            input_kwargs={
                "created_at": operation.created_at,
                "operation_id": operation.id,
                "run_id": operation.run_id,
                "job_id": job.id,
                "dataset_id": datasets[0].id,
                "num_bytes": None,
                "num_rows": None,
                "num_files": None,
            },
        )
        lineage.inputs.append(input)
        output = await create_output(
            async_session,
            output_kwargs={
                "created_at": operation.created_at,
                "operation_id": operation.id,
                "run_id": operation.run_id,
                "job_id": job.id,
                "dataset_id": datasets[1].id,
                "type": OutputType.APPEND,
                "num_bytes": None,
                "num_rows": None,
                "num_files": None,
            },
        )
        lineage.outputs.append(output)

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)
