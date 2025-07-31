from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
import pytest_asyncio
from faststream.kafka import KafkaBroker, KafkaMessage, TestKafkaBroker

from data_rentgen.http2kafka import broker_factory

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from data_rentgen.http2kafka.settings import Http2KafkaApplicationSettings


@pytest.fixture
def http2kafka_broker(http2kafka_app_settings: Http2KafkaApplicationSettings) -> KafkaBroker:
    return broker_factory(settings=http2kafka_app_settings)


@pytest_asyncio.fixture
async def http2kafka_handler_with_messages(
    broker: KafkaBroker,
    http2kafka_app_settings: Http2KafkaApplicationSettings,
):
    messages = []

    @broker.subscriber(http2kafka_app_settings.producer.main_topic)
    async def handler(message: KafkaMessage):
        messages.append(message)

    return handler, messages


@pytest_asyncio.fixture
async def test_http2kafka_broker(
    broker: KafkaBroker,
    http2kafka_handler_with_messages: Any,
) -> AsyncGenerator[KafkaBroker, None]:
    async with TestKafkaBroker(broker) as result:
        yield result
