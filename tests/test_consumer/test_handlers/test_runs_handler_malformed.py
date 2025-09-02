import pytest
from faststream.kafka import KafkaBroker

pytestmark = [pytest.mark.consumer, pytest.mark.asyncio]


@pytest.mark.asyncio
async def test_runs_handler_malformed(test_broker: KafkaBroker, malformed_handler_with_messages):
    await test_broker.publish(
        {"not": "valid event"},
        topic="input.runs",
        key="abc",
        headers={"cde": "efg"},
        timestamp_ms=1234,
        correlation_id="123",
    )

    malformed_handler, received = malformed_handler_with_messages
    await malformed_handler.wait_call(timeout=3)
    assert len(received) == 1

    message = received[0]
    raw_message = message.raw_message
    assert raw_message.topic == "input.runs__malformed"
    assert raw_message.key == "abc"
    assert raw_message.value == b'{"not":"valid event"}'
    assert raw_message.timestamp == 1234
    assert raw_message.headers == [
        (
            "content-type",
            b"application/json",
        ),
        (
            "correlation_id",
            b"123",
        ),
        (
            "cde",
            b"efg",
        ),
        (
            "reply_to",
            b"0-0-1234",
        ),
    ]
