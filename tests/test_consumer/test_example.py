import logging

import pytest
from faststream.kafka import KafkaBroker

pytestmark = [pytest.mark.consumer, pytest.mark.asyncio]


@pytest.mark.asyncio
async def test_broker_is_working(test_broker: KafkaBroker, caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.INFO):
        await test_broker.publish({"key": "value"}, "input")

    assert "Test handler, {'key': 'value'}" in caplog.text
