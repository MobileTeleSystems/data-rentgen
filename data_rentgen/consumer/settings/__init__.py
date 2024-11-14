# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from data_rentgen.consumer.settings.kafka import KafkaSettings
from data_rentgen.db.settings import DatabaseSettings
from data_rentgen.logging.settings import LoggingSettings


class ConsumerApplicationSettings(BaseSettings):
    """Data.Rentgen Kafka consumer settings.

    Application can be configured in 2 ways:

    * By explicitly passing ``settings`` object as an argument to :obj:`application_factory <data_rentgen.consumer.main.application_factory>`
    * By setting up environment variables matching a specific key.

        All environment variable names are written in uppercase and should be prefixed with ``DATA_RENTGEN__``.
        Nested items are delimited with ``__``.

    More details can be found in `Pydantic documentation <https://docs.pydantic.dev/latest/concepts/pydantic_settings/>`_.

    Examples
    --------

    .. code-block:: bash

        # same as settings.database.url = "postgresql+asyncpg://postgres:postgres@localhost:5432/data_rentgen"
        DATA_RENTGEN__DATABASE__URL=postgresql+asyncpg://postgres:postgres@localhost:5432/data_rentgen

        # same as settings.kafka.bootstrap_servers = "postgresql+asyncpg://postgres:postgres@localhost:5432/data_rentgen"
        DATA_RENTGEN__KAFKA__BOOTSTRAP_SERVERS=postgresql+asyncpg://postgres:postgres@localhost:5432/data_rentgen

        # same as settings.logging.preset = "json"
        DATA_RENTGEN__LOGGING__PRESET=json
    """

    database: DatabaseSettings = Field(description=":ref:`Database settings <configuration-database>`")
    kafka: KafkaSettings = Field(
        description=":ref:`Kafka settings <configuration-consumer-kafka>`",
    )
    logging: LoggingSettings = Field(
        default_factory=LoggingSettings,
        description=":ref:`Logging settings <configuration-consumer-logging>`",
    )

    model_config = SettingsConfigDict(env_prefix="DATA_RENTGEN__", env_nested_delimiter="__", extra="forbid")
