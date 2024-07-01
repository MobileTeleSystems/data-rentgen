# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import textwrap

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """Arrakis backend database settings.

    .. note::

        You can pass here any extra option supported by
        `SQLAlchemy Engine class <https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine>`_,
        even if it is not mentioned in documentation.

    Examples
    --------

    .. code-block:: bash

        ARRAKIS__DATABASE__URL=postgresql+asyncpg://postgres:postgres@localhost:5432/arrakis
        # custom option passed directly to engine factory
        ARRAKIS__DATABASE__POOL_PRE_PING=True
    """

    url: str = Field(
        description=textwrap.dedent(
            """
            Database connection URL.

            See `SQLAlchemy documentation <https://docs.sqlalchemy.org/en/20/core/engines.html#backend-specific-urls>`_

            .. warning:

                Only async drivers are supported, e.g. ``asyncpg``
            """,
        ),
    )

    model_config = SettingsConfigDict(env_prefix="ARRAKIS__DATABASE__", env_nested_delimiter="__", extra="allow")
