# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from typing import Annotated, cast

from aiokafka import ConsumerRecord
from faststream import Depends, Logger, NoCast
from faststream.kafka import KafkaMessage
from faststream.kafka.publisher import DefaultPublisher
from pydantic import TypeAdapter
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.consumer.extractors import BatchExtractor
from data_rentgen.consumer.saver import DatabaseSaver
from data_rentgen.dependencies.stub import Stub
from data_rentgen.openlineage.run_event import OpenLineageRunEvent

__all__ = [
    "runs_events_subscriber",
]

OpenLineageRunEventAdapter = TypeAdapter(OpenLineageRunEvent)


async def runs_events_subscriber(
    _events: NoCast[list[OpenLineageRunEvent]],
    batch: KafkaMessage,
    logger: Logger,
    publisher: Annotated[DefaultPublisher, Depends(Stub(DefaultPublisher))],
    session: Annotated[AsyncSession, Depends(Stub(AsyncSession))],
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

    saver = DatabaseSaver(session, logger)
    await saver.save(extracted)

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


async def report_malformed(
    messages: list[ConsumerRecord],
    message_id: str,
    correlation_id: str,
    publisher: DefaultPublisher,
):
    # Return malformed messages back to the broker
    for message in messages:
        headers: dict[str, str] = {}
        if message.headers:
            headers = {key: value.decode("utf-8") for key, value in message.headers}

        await publisher.publish(
            message.value,
            key=message.key,
            timestamp_ms=message.timestamp,
            headers=headers or None,
            reply_to=message_id,
            correlation_id=correlation_id,
        )
