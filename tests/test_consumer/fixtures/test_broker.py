from __future__ import annotations

from typing import TYPE_CHECKING

import pytest_asyncio
from faststream.kafka import KafkaBroker, TestKafkaBroker

from data_rentgen.consumer import broker_factory

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from data_rentgen.consumer.settings import ConsumerApplicationSettings


@pytest_asyncio.fixture(scope="session")
async def test_broker(consumer_app_settings: ConsumerApplicationSettings) -> AsyncGenerator[KafkaBroker, None]:
    broker = broker_factory(settings=consumer_app_settings)
    async with TestKafkaBroker(broker) as result:
        yield result
