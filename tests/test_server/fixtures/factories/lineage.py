from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime, timedelta
from typing import Callable

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import DatasetColumnRelationType, DatasetSymlinkType, Job, User
from data_rentgen.db.models.output import OutputType
from data_rentgen.db.utils.uuid import generate_static_uuid
from tests.test_server.fixtures.factories.dataset import create_dataset, make_symlink
from tests.test_server.fixtures.factories.input import create_input
from tests.test_server.fixtures.factories.job import create_job
from tests.test_server.fixtures.factories.location import create_location
from tests.test_server.fixtures.factories.operation import create_operation
from tests.test_server.fixtures.factories.output import create_output
from tests.test_server.fixtures.factories.relations import create_column_lineage, create_column_relation
from tests.test_server.fixtures.factories.run import create_run
from tests.test_server.fixtures.factories.schema import create_schema
from tests.test_server.utils.delete import clean_db
from tests.test_server.utils.lineage_result import LineageResult


@pytest_asyncio.fixture()
async def simple_lineage(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    job: Job,
    user: User,
) -> AsyncGenerator[LineageResult, None]:
    # Two independent operations, run twice:
    # J1 -> R1 -> O1, D1 -> O1 -> D2
    # J1 -> R1 -> O2, D3 -> O2 -> D4
    # J1 -> R2 -> O3, D1 -> O3 -> D2
    # J1 -> R2 -> O4, D3 -> O4 -> D4

    num_runs = 2
    num_operations = 2
    num_datasets = 4

    lineage = LineageResult(jobs=[job])
    async with async_session_maker() as async_session:
        created_at = datetime.now(tz=UTC)
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
                for i in range(num_operations)
            ]
            lineage.operations.extend(operations)

            dataset_locations = [await create_location(async_session) for _ in range(num_datasets)]
            datasets = [await create_dataset(async_session, location_id=location.id) for location in dataset_locations]
            lineage.datasets.extend(datasets)

            schema = await create_schema(async_session)

            inputs = [
                await create_input(
                    async_session,
                    input_kwargs={
                        "created_at": operation.created_at,
                        "operation_id": operation.id,
                        "run_id": run.id,
                        "job_id": job.id,
                        "dataset_id": datasets[2 * i].id,
                        "schema_id": schema.id,
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
                        "schema_id": schema.id,
                    },
                )
                for i, operation in enumerate(operations)
            ]
            lineage.outputs.extend(outputs)

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def three_days_lineage(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    job: Job,
    user: User,
) -> AsyncGenerator[LineageResult, None]:
    # Several of J -> R -> O, connected via same pair of datasets:
    # J0 -> R0 -> O0, D0 -> O0 -> D1
    # J0 -> R0 -> O1, D1 -> O1 -> D2
    # J1 -> R1 -> O2, D0 -> O2 -> D1
    # J1 -> R1 -> O3, D1 -> O3 -> D2
    # J2 -> R2 -> O4, D0 -> O4 -> D1
    # J2 -> R2 -> O5, D1 -> O5 -> D2
    # Runs are 1 day apart.

    lineage = LineageResult()
    lineage.jobs.append(job)
    created_at = datetime.now(tz=UTC)

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

            schema = await create_schema(async_session)

            inputs = [
                await create_input(
                    async_session,
                    input_kwargs={
                        "created_at": operation.created_at,
                        "operation_id": operation.id,
                        "run_id": operation.run_id,
                        "job_id": job.id,
                        "dataset_id": datasets[i].id,
                        "schema_id": schema.id,
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
                        "schema_id": schema.id,
                    },
                )
                for i, operation in enumerate(operations)
            ]
            lineage.outputs.extend(outputs)

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def lineage_with_depth(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
):
    # Three trees of J -> R -> O, connected via datasets:
    # J1 -> R1 -> O1, D1 -> O1 -> D2
    # J2 -> R2 -> O2, D2 -> O2 -> D3
    # J3 -> R3 -> O3, D3 -> O3 -> D4

    num_datasets = 4
    num_jobs = 3
    created_at = datetime.now(tz=UTC)

    lineage = LineageResult()
    async with async_session_maker() as async_session:
        dataset_locations = [await create_location(async_session) for _ in range(num_datasets)]
        datasets = [await create_dataset(async_session, location_id=location.id) for location in dataset_locations]
        lineage.datasets.extend(datasets)

        schema = await create_schema(async_session)

        # Create a job, run and operation with IO datasets.
        for i in range(num_jobs):
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
                    "schema_id": schema.id,
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
                    "schema_id": schema.id,
                },
            )
            lineage.outputs.append(output)

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def cyclic_lineage(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
):
    # Two trees of J -> R -> O, forming a cycle:
    # J1 -> R1 -> O1, D1 -> O1 -> D2
    # J2 -> R2 -> O2, D2 -> O2 -> D1

    num_datasets = 2
    num_jobs = 2
    created_at = datetime.now(tz=UTC)

    lineage = LineageResult()
    async with async_session_maker() as async_session:
        dataset_locations = [await create_location(async_session) for _ in range(num_datasets)]
        datasets = [await create_dataset(async_session, location_id=location.id) for location in dataset_locations]
        lineage.datasets.extend(datasets)

        schema = await create_schema(async_session)

        # Create a job, run and operation with IO datasets.
        for i in range(num_jobs):
            from_dataset, to_dataset = (datasets[0], datasets[1]) if i == 0 else (datasets[1], datasets[0])

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
                    "dataset_id": from_dataset.id,
                    "schema_id": schema.id,
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
                    "dataset_id": to_dataset.id,
                    "type": OutputType.APPEND,
                    "schema_id": schema.id,
                },
            )
            lineage.outputs.append(output)

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def duplicated_lineage(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
):
    # Two trees of J -> R -> O, interacting with the same dataset multiple times:
    # J0 -> R0 -> O0, D0 -> O0 -> D1
    # J0 -> R0 -> O1, D0 -> O1 -> D1
    # J0 -> R1 -> O2, D0 -> O2 -> D1
    # J0 -> R1 -> O3, D0 -> O3 -> D1
    # J1 -> R2 -> O4, D0 -> O4 -> D1
    # J1 -> R2 -> O5, D0 -> O5 -> D1
    # J1 -> R3 -> O6, D0 -> O6 -> D1
    # J1 -> R3 -> O7, D0 -> O7 -> D1

    num_datasets = 2
    num_jobs = 2
    runs_per_job = 2
    operations_per_run = 2
    created_at = datetime.now(tz=UTC)

    lineage = LineageResult()
    async with async_session_maker() as async_session:
        dataset_locations = [await create_location(async_session) for _ in range(num_datasets)]
        datasets = [await create_dataset(async_session, location_id=location.id) for location in dataset_locations]
        lineage.datasets.extend(datasets)

        schema = await create_schema(async_session)

        # Create a job, run and operation with IO datasets.
        for i in range(num_jobs):
            job_location = await create_location(async_session)
            job = await create_job(async_session, location_id=job_location.id)
            lineage.jobs.append(job)

            runs = [
                await create_run(
                    async_session,
                    run_kwargs={
                        "job_id": job.id,
                        "started_by_user_id": user.id,
                        "created_at": created_at + timedelta(seconds=i),
                    },
                )
                for _ in range(runs_per_job)
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
                for _ in range(operations_per_run)
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
                        "dataset_id": datasets[0].id,
                        "schema_id": schema.id,
                    },
                )
                for operation in operations
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
                        "dataset_id": datasets[1].id,
                        "type": OutputType.APPEND,
                        "schema_id": schema.id,
                    },
                )
                for operation in operations
            ]
            lineage.outputs.extend(outputs)

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def branchy_lineage(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
):
    # Three trees of J -> R -> O, connected via D3 and D6, but having other inputs & outputs:
    #          D0   D1
    #            \ /
    # J0 -> R0 -> O0 -> D2
    #              \
    #               D3  D4
    #                \ /
    #     J1 -> R1 -> O1 -> D5
    #                  \
    #                   D6  D7
    #                    \ /
    #         J2 -> R2 -> O2 -> D8
    #                      \
    #                       D9

    num_datasets = 10
    num_jobs = 3
    created_at = datetime.now(tz=UTC)

    lineage = LineageResult()
    async with async_session_maker() as async_session:
        dataset_locations = [await create_location(async_session) for _ in range(num_datasets)]
        datasets = [
            await create_dataset(async_session, location_id=location.id, dataset_kwargs={"name": f"dataset_{i}"})
            for i, location in enumerate(dataset_locations)
        ]
        lineage.datasets.extend(datasets)

        job_locations = [await create_location(async_session) for _ in range(num_jobs)]
        jobs = [
            await create_job(async_session, location_id=job_location.id, job_kwargs={"name": f"job_{i}"})
            for i, job_location in enumerate(job_locations)
        ]
        lineage.jobs.extend(jobs)

        runs = [
            await create_run(
                async_session,
                run_kwargs={
                    "job_id": job.id,
                    "started_by_user_id": user.id,
                    "created_at": created_at + timedelta(seconds=i),
                    "external_id": f"run_{i}",
                },
            )
            for i, job in enumerate(jobs)
        ]
        lineage.runs.extend(runs)

        operations = [
            await create_operation(
                async_session,
                operation_kwargs={
                    "run_id": run.id,
                    "created_at": run.created_at + timedelta(seconds=0.2),
                    "name": f"operation_{i}",
                },
            )
            for i, run in enumerate(runs)
        ]
        lineage.operations.extend(operations)

        schema = await create_schema(async_session)

        inputs = [
            await create_input(
                async_session,
                input_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": run.id,
                    "job_id": job.id,
                    "dataset_id": datasets[3 * i].id,
                    "schema_id": schema.id,
                },
            )
            for i, (operation, run, job) in enumerate(zip(operations, runs, jobs))
        ] + [
            await create_input(
                async_session,
                input_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": run.id,
                    "job_id": job.id,
                    "dataset_id": datasets[3 * i + 1].id,
                    "schema_id": schema.id,
                },
            )
            for i, (operation, run, job) in enumerate(zip(operations, runs, jobs))
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
                    "dataset_id": datasets[3 * i + 2].id,
                    "type": OutputType.APPEND,
                    "schema_id": schema.id,
                },
            )
            for i, (operation, run, job) in enumerate(zip(operations, runs, jobs))
        ] + [
            await create_output(
                async_session,
                output_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": run.id,
                    "job_id": job.id,
                    "dataset_id": datasets[3 * i + 3].id,
                    "type": OutputType.APPEND,
                    "schema_id": schema.id,
                },
            )
            for i, (operation, run, job) in enumerate(zip(operations, runs, jobs))
        ]
        lineage.outputs.extend(outputs)

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def lineage_with_symlinks(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
) -> AsyncGenerator[LineageResult, None]:
    # Three trees of J -> R -> O, connected to datasets via symlinks:
    # J1 -> R1 -> O1, D1 -> O1 -> D2S
    # J2 -> R2 -> O2, D2 -> O2 -> D3S
    # J3 -> R3 -> O3, D3 -> O2 -> D4S

    lineage = LineageResult()
    created_at = datetime.now(tz=UTC)
    num_datasets = 4
    num_jobs = 3

    async with async_session_maker() as async_session:
        dataset_locations = [
            await create_location(async_session, location_kwargs={"type": "hdfs"}) for _ in range(num_datasets)
        ]
        datasets = [await create_dataset(async_session, location_id=location.id) for location in dataset_locations]
        lineage.datasets.extend(datasets)

        symlink_locations = [
            await create_location(async_session, location_kwargs={"type": "hive"}) for _ in range(num_datasets)
        ]
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

        schema = await create_schema(async_session)

        # Make graphs
        for i in range(num_jobs):
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
                    "schema_id": schema.id,
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
                    "schema_id": schema.id,
                },
            )
            lineage.outputs.append(output)

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture
async def duplicated_lineage_with_column_lineage(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    duplicated_lineage: LineageResult,
) -> AsyncGenerator[AsyncSession, None]:
    # At this fixture we add column lineage to check relation types aggregation on different levels.
    # O0 will have two direct and indirect relations for same source-target columns.
    # O1 will have same source-target column as O0 but new relation type.
    # O2 will have same source-target column as O0 and O1 but new relation type.

    # Two trees of J -> R -> O, interacting with the same dataset multiple times:
    # J0 -> R0 -> O0, D0 -> O0 -> D1
    # J0 -> R0 -> O1, D0 -> O1 -> D1
    # J0 -> R1 -> O2, D0 -> O2 -> D1
    # J0 -> R1 -> O3, D0 -> O3 -> D1
    # J1 -> R2 -> O4, D0 -> O4 -> D1
    # J1 -> R2 -> O5, D0 -> O5 -> D1
    # J1 -> R3 -> O6, D0 -> O6 -> D1
    # J1 -> R3 -> O7, D0 -> O7 -> D1
    operation_relations_matrix = (
        (0, 0, DatasetColumnRelationType.IDENTITY),
        (0, 0, DatasetColumnRelationType.TRANSFORMATION),
        (1, 0, DatasetColumnRelationType.TRANSFORMATION_MASKING),
        (2, 1, DatasetColumnRelationType.AGGREGATION),
    )

    lineage = duplicated_lineage
    async with async_session_maker() as async_session:
        for operation, run, type in operation_relations_matrix:
            await create_column_relation(
                async_session,
                fingerprint=generate_static_uuid(type.name),
                column_relation_kwargs={
                    "type": type.value,
                    "source_column": "direct_source_column",
                    "target_column": "direct_target_column",
                },
            )
            await create_column_lineage(
                async_session,
                column_lineage_kwargs={
                    "created_at": lineage.operations[operation].created_at,
                    "operation_id": lineage.operations[operation].id,
                    "run_id": lineage.runs[run].id,
                    "job_id": lineage.jobs[0].id,
                    "source_dataset_id": lineage.datasets[0].id,
                    "target_dataset_id": lineage.datasets[1].id,
                    "fingerprint": generate_static_uuid(type.name),
                },
            )

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture
async def lineage_with_depth_and_with_column_lineage(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    lineage_with_depth: LineageResult,
) -> AsyncGenerator[AsyncSession, None]:
    # Three trees of J -> R -> O, connected via datasets:
    # J1 -> R1 -> O1, D1 -> O1 -> D2
    # J2 -> R2 -> O2, D2 -> O2 -> D3
    # J3 -> R3 -> O3, D3 -> O3 -> D4

    # Each Operation will have same column lineage.
    # So we can test not only depths but also same lineage for different operations, runs and jobs

    num_jobs = 3
    lineage = lineage_with_depth
    async with async_session_maker() as async_session:
        for i in range(num_jobs):
            # Direct
            await create_column_relation(
                async_session,
                fingerprint=generate_static_uuid(f"job_{i}"),
                column_relation_kwargs={
                    "type": DatasetColumnRelationType.AGGREGATION.value,
                    "source_column": "direct_source_column",
                    "target_column": "direct_target_column",
                },
            )
            # Indirect
            await create_column_relation(
                async_session,
                fingerprint=generate_static_uuid(f"job_{i}"),
                column_relation_kwargs={
                    "type": DatasetColumnRelationType.JOIN.value,
                    "source_column": "indirect_source_column",
                    "target_column": "",
                },
            )

            await create_column_lineage(
                async_session,
                column_lineage_kwargs={
                    "created_at": lineage.operations[i].created_at,
                    "operation_id": lineage.operations[i].id,
                    "run_id": lineage.runs[i].id,
                    "job_id": lineage.jobs[i].id,
                    "source_dataset_id": lineage.datasets[i].id,
                    "target_dataset_id": lineage.datasets[i + 1].id,
                    "fingerprint": generate_static_uuid(f"job_{i}"),
                },
            )

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)
