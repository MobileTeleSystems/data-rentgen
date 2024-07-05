import json
import logging
from pathlib import Path

import pytest
from faststream.kafka import KafkaBroker
from pydantic import ValidationError

RESOURCES_PATH = Path(__file__).parent.parent.joinpath("resources").resolve()

pytestmark = [pytest.mark.consumer, pytest.mark.asyncio]


@pytest.fixture
def events_airflow() -> list[dict]:
    lines = (RESOURCES_PATH / "events_airflow.jsonl").read_text().splitlines()
    return [json.loads(line) for line in lines]


@pytest.fixture
def events_spark() -> list[dict]:
    lines = (RESOURCES_PATH / "events_airflow.jsonl").read_text().splitlines()
    return [json.loads(line) for line in lines]


@pytest.mark.asyncio
async def test_runs_handler_spark(test_broker: KafkaBroker, caplog: pytest.LogCaptureFixture, events_spark: list[dict]):
    with caplog.at_level(logging.INFO):
        for event in events_spark:
            await test_broker.publish(event, "input.runs")

    assert "Successfully handled" in caplog.text


@pytest.mark.asyncio
async def test_runs_handler_airflow(
    test_broker: KafkaBroker,
    caplog: pytest.LogCaptureFixture,
    events_airflow: list[dict],
):
    with caplog.at_level(logging.INFO):
        for event in events_airflow:
            await test_broker.publish(event, "input.runs")

    assert "Successfully handled" in caplog.text


@pytest.mark.asyncio
async def test_runs_handler_error(test_broker: KafkaBroker):
    with pytest.raises(ValidationError):
        await test_broker.publish({"not": "valid event"}, "input.runs")
