# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from faststream import Depends
from faststream.kafka import KafkaRouter
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.consumer.extractors import (
    extract_dataset,
    extract_dataset_aliases,
    extract_dataset_symlinks,
    extract_input,
    extract_operation,
    extract_output,
    extract_run,
)
from data_rentgen.consumer.openlineage.dataset import OpenLineageDataset
from data_rentgen.consumer.openlineage.job_facets import OpenLineageJobType
from data_rentgen.consumer.openlineage.run_event import OpenLineageRunEvent
from data_rentgen.dependencies import Stub
from data_rentgen.dto import DatasetDTO, JobDTO, RunDTO, SchemaDTO, UserDTO
from data_rentgen.dto.dataset_symlink import DatasetSymlinkDTO
from data_rentgen.dto.location import LocationDTO
from data_rentgen.services.uow import UnitOfWork

router = KafkaRouter()


def get_unit_of_work(session: AsyncSession = Depends(Stub(AsyncSession))) -> UnitOfWork:
    return UnitOfWork(session)


@router.subscriber("input.runs", group_id="data-rentgen")
async def runs_handler(event: OpenLineageRunEvent, unit_of_work: UnitOfWork = Depends(get_unit_of_work)):
    if event.job.facets.jobType and event.job.facets.jobType.jobType == OpenLineageJobType.JOB:
        await handle_operation(event, unit_of_work)
    else:
        await handle_run(event, unit_of_work)


async def handle_run(event: OpenLineageRunEvent, unit_of_work: UnitOfWork) -> None:
    run_dto = extract_run(event)

    if run_dto.parent_run:
        async with unit_of_work:
            run_dto.parent_run = await create_or_update_run(run_dto.parent_run, unit_of_work)

    async with unit_of_work:
        await create_or_update_run(run_dto, unit_of_work)


async def handle_operation(event: OpenLineageRunEvent, unit_of_work: UnitOfWork) -> None:
    # To avoid issues when parallel consumer instances create the same object, and then fail at the end,
    # commit changes as soon as possible. Yes, this is quite slow, but it's fine for a prototype.
    # TODO: rewrite this to create objects in bulk.
    operation_dto = extract_operation(event)

    async with unit_of_work:
        operation_dto.run = await create_or_update_run(operation_dto.run, unit_of_work)

    async with unit_of_work:
        await unit_of_work.operation.create_or_update(operation_dto)

    inputs = []
    for raw_input in event.inputs:
        input_dto = extract_input(operation_dto, raw_input)
        input_dto.dataset = await handle_dataset(raw_input, unit_of_work)

        if input_dto.schema:
            async with unit_of_work:
                input_dto.schema = await get_or_create_schema(input_dto.schema, unit_of_work)

        inputs.append(input_dto)

    outputs = []
    for raw_output in event.outputs:
        output_dto = extract_output(operation_dto, raw_output)
        output_dto.dataset = await handle_dataset(raw_output, unit_of_work)

        if output_dto.schema:
            async with unit_of_work:
                output_dto.schema = await get_or_create_schema(output_dto.schema, unit_of_work)

        outputs.append(output_dto)

    # create inputs/outputs only as a last step, to avoid partial lineage graph. Operation should be either empty or not.
    async with unit_of_work:
        for input_dto in inputs:  # noqa: WPS440
            await unit_of_work.input.create_or_update(input_dto)

        for output_dto in outputs:  # noqa: WPS440
            await unit_of_work.output.create_or_update(output_dto)


async def handle_dataset(raw_dataset: OpenLineageDataset, unit_of_work: UnitOfWork) -> DatasetDTO:
    dataset_dto = extract_dataset(raw_dataset)
    dataset_dto = await create_or_update_dataset(dataset_dto, unit_of_work)
    datasets: dict[str, DatasetDTO] = {dataset_dto.full_name: dataset_dto}

    for raw_extra_dataset in extract_dataset_aliases(raw_dataset):
        datasets[raw_extra_dataset.full_name] = await create_or_update_dataset(raw_extra_dataset, unit_of_work)

    for dataset_symlink in extract_dataset_symlinks(raw_dataset):
        from_dataset = datasets[dataset_symlink.from_dataset.full_name]
        to_dataset = datasets[dataset_symlink.to_dataset.full_name]
        dataset_symilnk_dto = DatasetSymlinkDTO(from_dataset, to_dataset, dataset_symlink.type)
        await create_or_update_dataset_symlink(dataset_symilnk_dto, unit_of_work)

    return dataset_dto


async def create_or_update_location(location_dto: LocationDTO, unit_of_work: UnitOfWork) -> LocationDTO:
    location = await unit_of_work.location.create_or_update(location_dto)
    location_dto.id = location.id
    return location_dto


async def create_or_update_job(job_dto: JobDTO, unit_of_work: UnitOfWork) -> JobDTO:
    job_dto.location = await create_or_update_location(job_dto.location, unit_of_work)
    job = await unit_of_work.job.create_or_update(job_dto)
    job_dto.id = job.id
    return job_dto


async def get_or_create_user(user_dto: UserDTO, unit_of_work: UnitOfWork) -> UserDTO:
    user = await unit_of_work.user.get_or_create(user_dto)
    user_dto.id = user.id
    return user_dto


async def create_or_update_run(run_dto: RunDTO, unit_of_work: UnitOfWork) -> RunDTO:
    run_dto.job = await create_or_update_job(run_dto.job, unit_of_work)
    if run_dto.user:
        run_dto.user = await get_or_create_user(run_dto.user, unit_of_work)
    run = await unit_of_work.run.create_or_update(run_dto)
    run_dto.id = run.id
    return run_dto


async def create_or_update_dataset(dataset_dto: DatasetDTO, unit_of_work: UnitOfWork) -> DatasetDTO:
    dataset_dto.location = await create_or_update_location(dataset_dto.location, unit_of_work)
    dataset = await unit_of_work.dataset.create_or_update(dataset_dto)
    dataset_dto.id = dataset.id
    return dataset_dto


async def create_or_update_dataset_symlink(
    dataset_symlink_dto: DatasetSymlinkDTO,
    unit_of_work: UnitOfWork,
) -> DatasetSymlinkDTO:
    dataset_symlink = await unit_of_work.dataset_symlink.create_or_update(dataset_symlink_dto)
    dataset_symlink_dto.id = dataset_symlink.id
    return dataset_symlink_dto


async def get_or_create_schema(schema_dto: SchemaDTO, unit_of_work: UnitOfWork) -> SchemaDTO:
    schema = await unit_of_work.schema.get_or_create(schema_dto)
    schema_dto.id = schema.id
    return schema_dto
