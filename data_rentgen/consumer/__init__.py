# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import logging

from fast_depends import dependency_provider
from faststream import FastStream
from faststream.kafka import KafkaBroker
from sqlalchemy.ext.asyncio import AsyncSession

import data_rentgen
from data_rentgen.consumer.settings import ConsumerApplicationSettings
from data_rentgen.consumer.settings.security import get_broker_security
from data_rentgen.consumer.subscribers import runs_events_subscriber
from data_rentgen.db.factory import create_session_factory
from data_rentgen.logging.setup_logging import setup_logging

logger = logging.getLogger(__name__)


def broker_factory(settings: ConsumerApplicationSettings) -> KafkaBroker:
    broker = KafkaBroker(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        security=get_broker_security(settings.kafka.security),
        compression_type=settings.kafka.compression.value if settings.kafka.compression else None,
        client_id=f"data-rentgen-{data_rentgen.__version__}",
        logger=logger,
    )

    # register subscribers using settings
    consumer_settings = settings.consumer.model_dump(exclude={"topics_list", "topics_pattern"})
    broker.subscriber(
        *settings.consumer.topics_list,
        pattern=settings.consumer.topics_pattern,
        **consumer_settings,
        batch=True,
    )(runs_events_subscriber)

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
