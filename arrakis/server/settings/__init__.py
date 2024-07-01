# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from arrakis.db.settings import DatabaseSettings
from arrakis.logging.settings import LoggingSettings
from arrakis.server.settings.server import ServerSettings


class ServerApplicationSettings(BaseSettings):
    """Arrakis REST API settings.

    Application can be configured in 2 ways:

    * By explicitly passing ``settings`` object as an argument to :obj:`application_factory <arrakis.server.main.application_factory>`
    * By setting up environment variables matching a specific key.

        All environment variable names are written in uppercase and should be prefixed with ``ARRAKIS__``.
        Nested items are delimited with ``__``.

    More details can be found in `Pydantic documentation <https://docs.pydantic.dev/latest/concepts/pydantic_settings/>`_.

    Examples
    --------

    .. code-block:: bash

        # same as settings.database.url = "postgresql+asyncpg://postgres:postgres@localhost:5432/arrakis"
        ARRAKIS__DATABASE__URL=postgresql+asyncpg://postgres:postgres@localhost:5432/arrakis

        # same as settings.logging.preset = "json"
        ARRAKIS__LOGGING__PRESET=json

        # same as settings.server.debug = True
        ARRAKIS__SERVER__DEBUG=True
    """

    database: DatabaseSettings = Field(description=":ref:`Database settings <configuration-server-database>`")
    logging: LoggingSettings = Field(
        default_factory=LoggingSettings,
        description=":ref:`Logging settings <configuration-server-logging>`",
    )
    server: ServerSettings = Field(
        default_factory=ServerSettings,
        description=":ref:`Server settings <configuration-server>`",
    )

    model_config = SettingsConfigDict(env_prefix="ARRAKIS__", env_nested_delimiter="__", extra="forbid")
