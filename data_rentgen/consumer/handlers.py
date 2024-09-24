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
    extract_job,
    extract_operation,
    extract_output,
    extract_parent_run,
    extract_run,
    extract_run_user,
    extract_schema,
)
from data_rentgen.consumer.openlineage.dataset import OpenLineageDataset
from data_rentgen.consumer.openlineage.job_facets import OpenLineageJobType
from data_rentgen.consumer.openlineage.run_event import OpenLineageRunEvent
from data_rentgen.db.models import (
    Dataset,
    DatasetSymlink,
    DatasetSymlinkType,
    Input,
    Job,
    Operation,
    Output,
    Run,
    Schema,
    User,
)
from data_rentgen.dependencies import Stub
from data_rentgen.dto import (
    DatasetDTO,
    DatasetSymlinkTypeDTO,
    InputDTO,
    JobDTO,
    OperationDTO,
    OutputDTO,
    RunDTO,
    SchemaDTO,
    UserDTO,
)
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
    async with unit_of_work:
        parent_run = await get_or_create_parent_run(event, unit_of_work)

    async with unit_of_work:
        raw_job = extract_job(event.job)
        job = await create_or_update_job(raw_job, unit_of_work)

    async with unit_of_work:
        raw_user = extract_run_user(event)
        user: User | None = None
        if raw_user:
            user = await get_or_create_user(raw_user, unit_of_work)

    async with unit_of_work:
        raw_run = extract_run(event)
        await create_or_update_run(raw_run, job, parent_run, user, unit_of_work)


async def handle_operation(event: OpenLineageRunEvent, unit_of_work: UnitOfWork) -> None:
    # To avoid issues when parallel consumer instances create the same object, and then fail at the end,
    # commit changes as soon as possible. Yes, this is quite slow, but it's fine for a prototype.
    # TODO: rewrite this to create objects in bulk.
    async with unit_of_work:
        run = await get_or_create_parent_run(event, unit_of_work)

    async with unit_of_work:
        raw_operation = extract_operation(event)
        operation = await create_or_update_operation(raw_operation, run, unit_of_work)

    input_components = []
    for raw_input in event.inputs:
        async with unit_of_work:
            dataset = await handle_dataset(raw_input, unit_of_work)
        async with unit_of_work:
            schema = await handle_schema(raw_input, unit_of_work)

        input = extract_input(raw_input)
        input_components.append((input, operation, run, dataset, schema))

    output_components = []
    for raw_output in event.outputs:
        async with unit_of_work:
            dataset = await handle_dataset(raw_output, unit_of_work)
        async with unit_of_work:
            schema = await handle_schema(raw_output, unit_of_work)

        output = extract_output(raw_output)
        output_components.append((output, operation, run, dataset, schema))

    # create inputs/outputs only as a last step, to avoid partial lineage graph. Operation should be either empty or not.
    async with unit_of_work:
        for input, operation, run, dataset, schema in input_components:  # noqa: WPS440
            await create_or_update_input(input, operation, run, dataset, schema, unit_of_work)  # type: ignore[arg-type]

        for output, operation, run, dataset, schema in output_components:  # noqa: WPS440
            await create_or_update_output(output, operation, run, dataset, schema, unit_of_work)  # type: ignore[arg-type]


async def handle_dataset(dataset: OpenLineageDataset, unit_of_work: UnitOfWork) -> Dataset:
    raw_dataset = extract_dataset(dataset)
    result = await get_or_create_dataset(raw_dataset, unit_of_work)
    datasets: dict[str, Dataset] = {raw_dataset.full_name: result}

    for raw_extra_dataset in extract_dataset_aliases(dataset):
        datasets[raw_extra_dataset.full_name] = await get_or_create_dataset(raw_extra_dataset, unit_of_work)

    for dataset_symlink in extract_dataset_symlinks(dataset):
        from_dataset = datasets[dataset_symlink.from_dataset.full_name]
        to_dataset = datasets[dataset_symlink.to_dataset.full_name]
        await get_or_create_dataset_symlink(
            from_dataset,
            to_dataset,
            dataset_symlink.type,
            unit_of_work,
        )

    return result


async def handle_schema(dataset: OpenLineageDataset, unit_of_work: UnitOfWork) -> Schema | None:
    schema = extract_schema(dataset)
    if not schema:
        return None
    async with unit_of_work:
        return await get_or_create_schema(schema, unit_of_work)


async def get_or_create_parent_run(event: OpenLineageRunEvent, unit_of_work: UnitOfWork) -> Run | None:
    if not event.run.facets.parent:
        return None

    raw_parent_run = extract_parent_run(event.run.facets.parent)
    parent_job = await create_or_update_job(raw_parent_run.job, unit_of_work)
    return await unit_of_work.run.get_or_create_minimal(raw_parent_run, parent_job.id)


async def create_or_update_job(job: JobDTO, unit_of_work: UnitOfWork) -> Job:
    matching_location = await unit_of_work.location.get_or_create(job.location)
    return await unit_of_work.job.create_or_update(job, matching_location.id)


async def get_or_create_user(user: UserDTO, unit_of_work: UnitOfWork) -> User:
    return await unit_of_work.user.get_or_create(user)


async def create_or_update_run(
    run: RunDTO,
    job: Job,
    parent_run: Run | None,
    user: User | None,
    unit_of_work: UnitOfWork,
) -> Run:
    parent_run_id = parent_run.id if parent_run else None
    user_id = user.id if user else None
    return await unit_of_work.run.create_or_update(run, job.id, parent_run_id, user_id)


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
    return await unit_of_work.dataset_symlink.create_or_update(
        from_dataset.id,
        to_dataset.id,
        DatasetSymlinkType(symlink_type),
    )


async def get_or_create_schema(schema: SchemaDTO, unit_of_work: UnitOfWork) -> Schema:
    return await unit_of_work.schema.get_or_create(schema)


async def create_or_update_input(
    input: InputDTO,
    operation: Operation,
    run: Run,
    dataset: Dataset,
    schema: Schema | None,
    unit_of_work: UnitOfWork,
) -> Input:
    schema_id = schema.id if schema else None
    return await unit_of_work.input.create_or_update(
        input,
        operation.id,
        run.id,
        run.job_id,
        dataset_id=dataset.id,
        schema_id=schema_id,
    )


async def create_or_update_output(
    output: OutputDTO,
    operation: Operation,
    run: Run,
    dataset: Dataset,
    schema: Schema | None,
    unit_of_work: UnitOfWork,
) -> Output:
    schema_id = schema.id if schema else None
    return await unit_of_work.output.create_or_update(output, operation.id, run.id, run.job_id, dataset.id, schema_id)
