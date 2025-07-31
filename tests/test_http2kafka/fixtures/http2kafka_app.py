from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest

from data_rentgen.http2kafka import application_factory

if TYPE_CHECKING:
    from fastapi import FastAPI
    from faststream.kafka import TestKafkaBroker

    from data_rentgen.http2kafka.settings import Http2KafkaApplicationSettings


@pytest.fixture
def http2kafka_app(
    http2kafka_app_settings: Http2KafkaApplicationSettings,
    test_http2kafka_broker: TestKafkaBroker,
) -> FastAPI:
    with patch("data_rentgen.http2kafka.broker_factory", return_value=test_http2kafka_broker):
        return application_factory(settings=http2kafka_app_settings)
