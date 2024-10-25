# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from faststream import Depends, Logger
from faststream.kafka import KafkaRouter
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.consumer.extractors import BatchExtractionResult, extract_batch
from data_rentgen.consumer.openlineage.run_event import OpenLineageRunEvent
from data_rentgen.dependencies import Stub
from data_rentgen.services.uow import UnitOfWork

router = KafkaRouter()


def get_unit_of_work(session: AsyncSession = Depends(Stub(AsyncSession))) -> UnitOfWork:
    return UnitOfWork(session)


@router.subscriber("input.runs", group_id="data-rentgen", batch=True)
async def runs_handler(
    events: list[OpenLineageRunEvent],
    logger: Logger,
    unit_of_work: UnitOfWork = Depends(get_unit_of_work),
):
    logger.info("Got %d events", len(events))
    extracted = extract_batch(events)
    logger.info("Extracted: %r", extracted)

    logger.info("Saving to database")
    await save_to_db(extracted, unit_of_work, logger)
    logger.info("Saved successfully")


async def save_to_db(data: BatchExtractionResult, unit_of_work: UnitOfWork, logger: Logger) -> None:  # noqa: WPS217
    # To avoid issues when parallel consumer instances create the same object, and then fail at the end,
    # commit changes as soon as possible. Yes, this is quite slow, but it's fine for a prototype.
    # TODO: rewrite this to create objects in batch.

    logger.debug("Creating locations")
    for location_dto in data.locations():
        async with unit_of_work:
            location = await unit_of_work.location.create_or_update(location_dto)
            location_dto.id = location.id

    logger.debug("Creating datasets")
    for dataset_dto in data.datasets():
        async with unit_of_work:
            dataset = await unit_of_work.dataset.create_or_update(dataset_dto)
            dataset_dto.id = dataset.id

    logger.debug("Creating symlinks")
    for dataset_symlink_dto in data.dataset_symlinks():
        async with unit_of_work:
            dataset_symlink = await unit_of_work.dataset_symlink.create_or_update(dataset_symlink_dto)
            dataset_symlink_dto.id = dataset_symlink.id

    logger.debug("Creating jobs")
    for job_dto in data.jobs():
        async with unit_of_work:
            job = await unit_of_work.job.create_or_update(job_dto)
            job_dto.id = job.id

    logger.debug("Creating users")
    for user_dto in data.users():
        async with unit_of_work:
            user = await unit_of_work.user.get_or_create(user_dto)
            user_dto.id = user.id

    logger.debug("Creating schemas")
    for schema_dto in data.schemas():
        async with unit_of_work:
            schema = await unit_of_work.schema.get_or_create(schema_dto)
            schema_dto.id = schema.id

    logger.debug("Creating runs")
    for run_dto in data.runs():
        async with unit_of_work:
            await unit_of_work.run.create_or_update(run_dto)

    logger.debug("Creating operations")
    for operation_dto in data.operations():
        async with unit_of_work:
            await unit_of_work.operation.create_or_update(operation_dto)

    logger.debug("Creating inputs")
    for input_dto in data.inputs():
        async with unit_of_work:
            input = await unit_of_work.input.create_or_update(input_dto)
            input_dto.id = input.id

    logger.debug("Creating outputs")
    for output_dto in data.outputs():
        async with unit_of_work:
            output = await unit_of_work.output.create_or_update(output_dto)
            output_dto.id = output.id
