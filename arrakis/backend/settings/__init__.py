# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from arrakis.backend.settings.database import DatabaseSettings
from arrakis.backend.settings.server import ServerSettings


class Settings(BaseSettings):
    """Arrakis backend settings.

    Backend can be configured in 2 ways:

    * By explicitly passing ``settings`` object as an argument to :obj:`application_factory <arrakis.backend.main.application_factory>`
    * By setting up environment variables matching a specific key.

        All environment variable names are written in uppercase and should be prefixed with ``ARRAKIS__``.
        Nested items are delimited with ``__``.

    More details can be found in `Pydantic documentation <https://docs.pydantic.dev/latest/concepts/pydantic_settings/>`_.

    Examples
    --------

    .. code-block:: bash

        # same as settings.database.url = "postgresql+asyncpg://postgres:postgres@localhost:5432/arrakis"
        ARRAKIS__DATABASE__URL=postgresql+asyncpg://postgres:postgres@localhost:5432/arrakis

        # same as settings.server.debug = True
        ARRAKIS__SERVER__DEBUG=True
    """

    database: DatabaseSettings = Field(description=":ref:`Database settings <backend-configuration-database>`")
    server: ServerSettings = Field(
        default_factory=ServerSettings,
        description=":ref:`Server settings <backend-configuration>`",
    )

    model_config = SettingsConfigDict(env_prefix="ARRAKIS__", env_nested_delimiter="__")
