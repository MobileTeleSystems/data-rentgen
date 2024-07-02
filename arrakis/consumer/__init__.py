# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import logging

from faststream import FastStream
from faststream.kafka import KafkaBroker

import arrakis
from arrakis.consumer.handlers import router
from arrakis.consumer.settings import ConsumerApplicationSettings
from arrakis.consumer.settings.security import get_broker_security
from arrakis.logging.setup_logging import setup_logging

logger = logging.getLogger(__name__)


def broker_factory(settings: ConsumerApplicationSettings) -> KafkaBroker:
    broker = KafkaBroker(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        security=get_broker_security(settings.kafka.security),
        logger=logger,
    )
    broker.include_router(router)
    return broker


def application_factory(settings: ConsumerApplicationSettings) -> FastStream:
    return FastStream(
        broker=broker_factory(settings),
        title="Arrakis",
        description="Arrakis is a nextgen DataLineage service",
        version=arrakis.__version__,
        logger=logger,
    )


def get_application():
    settings = ConsumerApplicationSettings()
    setup_logging(settings.logging)
    return application_factory(settings=settings)
