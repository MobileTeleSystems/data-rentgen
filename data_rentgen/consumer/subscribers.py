# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from typing import cast

from aiokafka import ConsumerRecord
from faststream import Depends, Logger, NoCast
from faststream.kafka import KafkaMessage
from faststream.kafka.publisher.asyncapi import AsyncAPIDefaultPublisher
from pydantic import TypeAdapter
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.consumer.extractors import BatchExtractionResult, BatchExtractor
from data_rentgen.consumer.openlineage.run_event import OpenLineageRunEvent
from data_rentgen.dependencies import Stub
from data_rentgen.services.uow import UnitOfWork

__all__ = [
    "runs_events_subscriber",
]

OpenLineageRunEventAdapter = TypeAdapter(OpenLineageRunEvent)


async def runs_events_subscriber(
    _events: NoCast[list[OpenLineageRunEvent]],
    batch: KafkaMessage,
    logger: Logger,
    publisher: AsyncAPIDefaultPublisher = Depends(Stub(AsyncAPIDefaultPublisher)),
    session: AsyncSession = Depends(Stub(AsyncSession)),
):
    message_id = batch.message_id
    correlation_id = batch.correlation_id
    messages = cast(tuple[ConsumerRecord, ...], batch.raw_message)  # https://github.com/airtai/faststream/issues/2102
    del batch  # free memory

    total_bytes = sum(len(message.value or "") for message in messages)
    logger.info("Got %d messages (%dKiB)", len(messages), total_bytes / 1024)

    extractor = BatchExtractor()
    malformed: list[ConsumerRecord] = []
    logger.info("Extracting events")
    async for parsed, raw in parse_messages(messages, logger):
        if parsed:
            extractor.add_events([parsed])
        if raw:
            malformed.append(raw)
    del messages  # free memory

    extracted = extractor.result
    logger.info("Got %r", extracted)

    logger.info("Saving to database")
    await save_to_db(extracted, session, logger)
    logger.info("Saved successfully")

    if malformed:
        logger.warning("Malformed messages: %d", len(malformed))
        await report_malformed(malformed, message_id, correlation_id, publisher)


async def parse_messages(
    messages: tuple[ConsumerRecord, ...],
    logger: Logger,
) -> AsyncGenerator[tuple[OpenLineageRunEvent, None] | tuple[None, ConsumerRecord]]:
    # OpenLineage models are heavy, parsing is CPU bound task which may take some time.
    # Blocking event loop is not a good idea, so we need to await sometimes,
    for message in messages:
        try:
            if message.value is None:
                msg = "Message value cannot be empty"
                raise ValueError(msg)  # noqa: TRY301

            yield OpenLineageRunEventAdapter.validate_json(message.value), None
        except (ValueError, TypeError):
            logger.exception(
                "Failed to parse message: ConsumerRecord(topic=%r, partition=%d, offset=%d)",
                message.topic,
                message.partition,
                message.offset,
            )
            yield None, message

        await asyncio.sleep(0)


async def save_to_db(
    data: BatchExtractionResult,
    session: AsyncSession,
    logger: Logger,
) -> None:
    # To avoid deadlocks when parallel consumer instances insert/update the same row,
    # commit changes for each row instead of committing the whole batch. Yes, this cloud be slow.

    unit_of_work = UnitOfWork(session)

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

    logger.debug("Creating job types")
    for job_type_dto in data.job_types():
        async with unit_of_work:
            job_type = await unit_of_work.job_type.get_or_create(job_type_dto)
            job_type_dto.id = job_type.id

    logger.debug("Creating jobs")
    for job_dto in data.jobs():
        async with unit_of_work:
            job = await unit_of_work.job.create_or_update(job_dto)
            job_dto.id = job.id

    logger.debug("Creating sql queries")
    for sql_query_dto in data.sql_queries():
        async with unit_of_work:
            sql_query = await unit_of_work.sql_query.get_or_create(sql_query_dto)
            sql_query_dto.id = sql_query.id

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

    # Some events related to specific run are send to the same Kafka partition,
    # but at the same time we have parent_run which may be already inserted/updated by other worker
    # (Kafka key maybe different for run and it's parent).
    # In this case we cannot insert all the rows in one transaction, as it may lead to deadlocks.
    logger.debug("Creating runs")
    for run_dto in data.runs():
        async with unit_of_work:
            await unit_of_work.run.create_or_update(run_dto)

    # All events related to same operation are always send to the same Kafka partition,
    # so other workers never insert/update the same operation in parallel.
    # These rows can be inserted/updated in bulk, in one transaction.
    async with unit_of_work:
        logger.debug("Creating operations")
        await unit_of_work.operation.create_or_update_bulk(data.operations())

        logger.debug("Creating inputs")
        await unit_of_work.input.create_or_update_bulk(data.inputs())

        logger.debug("Creating outputs")
        await unit_of_work.output.create_or_update_bulk(data.outputs())

    # If something went wrong here, at least we will have inputs/outputs
    async with unit_of_work:
        column_lineage = data.column_lineage()
        logger.debug("Creating dataset column relations")
        await unit_of_work.dataset_column_relation.create_bulk_for_column_lineage(column_lineage)

        logger.debug("Creating column lineage")
        await unit_of_work.column_lineage.create_bulk(column_lineage)


async def report_malformed(
    messages: list[ConsumerRecord],
    message_id: str,
    correlation_id: str,
    publisher: AsyncAPIDefaultPublisher,
):
    # Return malformed messages back to the broker
    for message in messages:
        headers: dict[str, str] = {}
        if message.headers:
            headers = {key: value.decode("utf-8") for key, value in message.headers}

        await publisher.publish(
            message.value,
            key=message.key,
            partition=message.partition,
            timestamp_ms=message.timestamp,
            headers=headers or None,
            reply_to=message_id,
            correlation_id=correlation_id,
        )
