# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from faststream import Depends
from faststream.kafka import KafkaRouter
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.consumer.extractors import (
    extract_dataset_symlinks,
    extract_datasets,
    extract_interaction_schema,
    extract_job,
    extract_operation,
    extract_parent_run,
    extract_run,
)
from data_rentgen.consumer.openlineage.job_facets import OpenLineageJobType
from data_rentgen.consumer.openlineage.run_event import OpenLineageRunEvent
from data_rentgen.db.models import (
    Dataset,
    DatasetSymlink,
    DatasetSymlinkType,
    Job,
    Operation,
    Run,
)
from data_rentgen.db.models.schema import Schema
from data_rentgen.dependencies import Stub
from data_rentgen.dto import (
    DatasetDTO,
    DatasetSymlinkDTO,
    DatasetSymlinkTypeDTO,
    JobDTO,
    OperationDTO,
    RunDTO,
)
from data_rentgen.dto.schema import SchemaDTO
from data_rentgen.services.uow import UnitOfWork

router = KafkaRouter()


def get_unit_of_work(session: AsyncSession = Depends(Stub(AsyncSession))) -> UnitOfWork:
    return UnitOfWork(session)


@router.subscriber("input.runs")
async def runs_handler(event: OpenLineageRunEvent, unit_of_work: UnitOfWork = Depends(get_unit_of_work)):
    if event.job.facets.jobType and event.job.facets.jobType.jobType == OpenLineageJobType.JOB:
        await handle_operation(event, unit_of_work)
    else:
        await handle_run(event, unit_of_work)


async def handle_run(event: OpenLineageRunEvent, unit_of_work: UnitOfWork) -> None:
    async with unit_of_work:
        parent_run = await get_or_create_parent_run(event, unit_of_work)

    async with unit_of_work:
        raw_job = extract_job(event.job)
        job = await get_or_create_job(raw_job, unit_of_work)

    async with unit_of_work:
        raw_run = extract_run(event)
        await create_or_update_run(raw_run, job, parent_run, unit_of_work)


async def handle_operation(event: OpenLineageRunEvent, unit_of_work: UnitOfWork) -> None:
    async with unit_of_work:
        run = await get_or_create_parent_run(event, unit_of_work)

    async with unit_of_work:
        raw_operation = extract_operation(event)
        await create_or_update_operation(raw_operation, run, unit_of_work)

    async with unit_of_work:
        datasets = await handle_datasets(event, unit_of_work)
        await handle_dataset_symlinks(event, datasets, unit_of_work)

    async with unit_of_work:
        await handle_interaction_schemas(event, unit_of_work)


async def handle_datasets(event: OpenLineageRunEvent, unit_of_work: UnitOfWork) -> dict[str, Dataset]:
    raw_datasets: list[DatasetDTO] = []
    for input in event.inputs:
        raw_datasets.extend(extract_datasets(input))
    for output in event.outputs:
        raw_datasets.extend(extract_datasets(output))

    result: dict[str, Dataset] = {}
    for raw_dataset in raw_datasets:
        dataset = await get_or_create_dataset(raw_dataset, unit_of_work)
        result[raw_dataset.full_name] = dataset
    return result


async def handle_dataset_symlinks(
    event: OpenLineageRunEvent,
    datasets: dict[str, Dataset],
    unit_of_work: UnitOfWork,
) -> None:
    dataset_symlinks: list[DatasetSymlinkDTO] = []
    for input in event.inputs:
        dataset_symlinks.extend(extract_dataset_symlinks(input))
    for output in event.outputs:
        dataset_symlinks.extend(extract_dataset_symlinks(output))

    for dataset_symlink in dataset_symlinks:
        from_dataset = datasets[dataset_symlink.from_dataset.full_name]
        to_dataset = datasets[dataset_symlink.to_dataset.full_name]
        await get_or_create_dataset_symlink(
            from_dataset,
            to_dataset,
            dataset_symlink.type,
            unit_of_work,
        )


async def handle_interaction_schemas(
    event: OpenLineageRunEvent,
    unit_of_work: UnitOfWork,
) -> None:
    schemas: list[SchemaDTO] = []
    for input in event.inputs:
        schema = extract_interaction_schema(input)
        if schema:
            schemas.append(schema)

    for output in event.outputs:
        schema = extract_interaction_schema(output)
        if schema:
            schemas.append(schema)

    for raw_schema in schemas:
        await get_or_create_schema(raw_schema, unit_of_work)


async def get_or_create_parent_run(event: OpenLineageRunEvent, unit_of_work: UnitOfWork) -> Run | None:
    if not event.run.facets.parent:
        return None

    raw_parent_run = extract_parent_run(event.run.facets.parent)
    parent_job = await get_or_create_job(raw_parent_run.job, unit_of_work)
    return await unit_of_work.run.get_or_create_minimal(raw_parent_run, parent_job.id)


async def get_or_create_job(job: JobDTO, unit_of_work: UnitOfWork) -> Job:
    matching_location = await unit_of_work.location.get_or_create(job.location)
    return await unit_of_work.job.get_or_create(job, matching_location.id)


async def create_or_update_run(run: RunDTO, job: Job, parent_run: Run | None, unit_of_work: UnitOfWork) -> Run:
    parent_run_id = parent_run.id if parent_run else None
    return await unit_of_work.run.create_or_update(run, job.id, parent_run_id)


async def create_or_update_operation(operation: OperationDTO, run: Run | None, unit_of_work: UnitOfWork) -> Operation:
    run_id = run.id if run else None
    return await unit_of_work.operation.create_or_update(operation, run_id)


async def get_or_create_dataset(dataset: DatasetDTO, unit_of_work: UnitOfWork) -> Dataset:
    matching_location = await unit_of_work.location.get_or_create(dataset.location)
    return await unit_of_work.dataset.create_or_update(dataset, matching_location.id)


async def get_or_create_dataset_symlink(
    from_dataset: Dataset,
    to_dataset: Dataset,
    symlink_type: DatasetSymlinkTypeDTO,
    unit_of_work: UnitOfWork,
) -> DatasetSymlink:
    return await unit_of_work.dataset.create_or_update_symlink(
        from_dataset.id,
        to_dataset.id,
        DatasetSymlinkType(symlink_type),
    )


async def get_or_create_schema(schema: SchemaDTO, unit_of_work: UnitOfWork) -> Schema:
    return await unit_of_work.schema.get_or_create(schema)
