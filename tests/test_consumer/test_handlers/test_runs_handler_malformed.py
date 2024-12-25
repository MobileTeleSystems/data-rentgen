import pytest
from faststream.kafka import KafkaBroker
from pydantic import ValidationError

pytestmark = [pytest.mark.consumer, pytest.mark.asyncio]


@pytest.mark.asyncio
async def test_runs_handler_malformed(test_broker: KafkaBroker):
    with pytest.raises(ValidationError):
        await test_broker.publish({"not": "valid event"}, "input.runs")
