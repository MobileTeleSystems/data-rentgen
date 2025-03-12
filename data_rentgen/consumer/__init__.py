# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import logging
from contextlib import asynccontextmanager

import anyio
from fast_depends import dependency_provider
from faststream import ContextRepo, FastStream
from faststream._compat import ExceptionGroup
from faststream.kafka import KafkaBroker
from sqlalchemy.ext.asyncio import AsyncSession

import data_rentgen
from data_rentgen.consumer.settings import ConsumerApplicationSettings
from data_rentgen.consumer.subscribers import runs_events_subscriber
from data_rentgen.db.factory import create_session_factory
from data_rentgen.logging.setup_logging import setup_logging

logger = logging.getLogger(__name__)


def broker_factory(settings: ConsumerApplicationSettings) -> KafkaBroker:
    broker = KafkaBroker(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        security=settings.kafka.security.to_security(),
        compression_type=settings.kafka.compression.value if settings.kafka.compression else None,
        client_id=f"data-rentgen-{data_rentgen.__version__}",
        logger=logger,
        **settings.kafka.security.extra_broker_kwargs(),
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
    @asynccontextmanager
    async def security_lifespan(context: ContextRepo):
        try:
            async with anyio.create_task_group() as tg:
                await settings.kafka.security.initialize()
                tg.start_soon(settings.kafka.security.refresh)

                yield

                await settings.kafka.security.destroy()
                tg.cancel_scope.cancel()
        except ExceptionGroup as e:
            for exception in e.exceptions:
                raise exception from None

    return FastStream(
        broker=broker_factory(settings),
        lifespan=security_lifespan,
        title="Data.Rentgen",
        description="Data.Rentgen is a nextgen DataLineage service",
        version=data_rentgen.__version__,
        logger=logger,
    )


def get_application():
    settings = ConsumerApplicationSettings()
    setup_logging(settings.logging)
    return application_factory(settings=settings)
