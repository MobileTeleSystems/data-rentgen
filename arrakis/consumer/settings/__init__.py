# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from arrakis.consumer.settings.kafka import KafkaSettings
from arrakis.db.settings import DatabaseSettings
from arrakis.logging.settings import LoggingSettings


class ConsumerApplicationSettings(BaseSettings):
    """Arrakis Kafka consumer settings.

    Application can be configured in 2 ways:

    * By explicitly passing ``settings`` object as an argument to :obj:`application_factory <arrakis.consumer.main.application_factory>`
    * By setting up environment variables matching a specific key.

        All environment variable names are written in uppercase and should be prefixed with ``ARRAKIS__``.
        Nested items are delimited with ``__``.

    More details can be found in `Pydantic documentation <https://docs.pydantic.dev/latest/concepts/pydantic_settings/>`_.

    Examples
    --------

    .. code-block:: bash

        # same as settings.database.url = "postgresql+asyncpg://postgres:postgres@localhost:5432/arrakis"
        ARRAKIS__DATABASE__URL=postgresql+asyncpg://postgres:postgres@localhost:5432/arrakis

        # same as settings.kafka.bootstrap_servers = "postgresql+asyncpg://postgres:postgres@localhost:5432/arrakis"
        ARRAKIS__KAFKA__BOOTSTRAP_SERVERS=postgresql+asyncpg://postgres:postgres@localhost:5432/arrakis

        # same as settings.logging.preset = "json"
        ARRAKIS__LOGGING__PRESET=json
    """

    database: DatabaseSettings = Field(description=":ref:`Database settings <configuration-consumer-database>`")
    kafka: KafkaSettings = Field(
        description=":ref:`Kafka settings <configuration-consumer-kafka>`",
    )
    logging: LoggingSettings = Field(
        default_factory=LoggingSettings,
        description=":ref:`Logging settings <configuration-consumer-logging>`",
    )

    model_config = SettingsConfigDict(env_prefix="ARRAKIS__", env_nested_delimiter="__", extra="forbid")
