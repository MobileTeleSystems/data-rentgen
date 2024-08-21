# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import logging

from fast_depends import dependency_provider
from faststream import FastStream
from faststream.kafka import KafkaBroker
from sqlalchemy.ext.asyncio import AsyncSession

import data_rentgen
from data_rentgen.consumer.handlers import router
from data_rentgen.consumer.settings import ConsumerApplicationSettings
from data_rentgen.consumer.settings.security import get_broker_security
from data_rentgen.db.factory import create_session_factory
from data_rentgen.logging.setup_logging import setup_logging

logger = logging.getLogger(__name__)


def broker_factory(settings: ConsumerApplicationSettings) -> KafkaBroker:
    broker = KafkaBroker(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        security=get_broker_security(settings.kafka.security),
        compression_type=settings.kafka.compression.value if settings.kafka.compression else None,
        logger=logger,
    )
    broker.include_router(router)
    dependency_provider.override(AsyncSession, create_session_factory(settings.database))
    return broker


def application_factory(settings: ConsumerApplicationSettings) -> FastStream:
    return FastStream(
        broker=broker_factory(settings),
        title="Data.Rentgen",
        description="Data.Rentgen is a nextgen DataLineage service",
        version=data_rentgen.__version__,
        logger=logger,
    )


def get_application():
    settings = ConsumerApplicationSettings()
    setup_logging(settings.logging)
    return application_factory(settings=settings)
