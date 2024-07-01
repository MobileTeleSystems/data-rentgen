# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import List

from pydantic import BaseModel, ConfigDict, Field


class CORSSettings(BaseModel):
    """CORS Middleware Settings.

    See `CORSMiddleware <https://www.starlette.io/middleware/#corsmiddleware>`_ documentation.

    .. note::

        You can pass here any extra option supported by ``CORSMiddleware``,
        even if it is not mentioned in documentation.

    Examples
    --------

    For development environment:

    .. code-block:: bash

        ARRAKIS__SERVER__CORS__ENABLED=True
        ARRAKIS__SERVER__CORS__ALLOW_ORIGINS=["*"]
        ARRAKIS__SERVER__CORS__ALLOW_METHODS=["*"]
        ARRAKIS__SERVER__CORS__ALLOW_HEADERS=["*"]
        ARRAKIS__SERVER__CORS__EXPOSE_HEADERS=["X-Request-ID"]

    For production environment:

    .. code-block:: bash

        ARRAKIS__SERVER__CORS__ENABLED=True
        ARRAKIS__SERVER__CORS__ALLOW_ORIGINS=["production.example.com"]
        ARRAKIS__SERVER__CORS__ALLOW_METHODS=["GET"]
        ARRAKIS__SERVER__CORS__ALLOW_HEADERS=["X-Request-ID", "X-Request-With"]
        ARRAKIS__SERVER__CORS__EXPOSE_HEADERS=["X-Request-ID"]
        # custom option passed directly to middleware
        ARRAKIS__SERVER__CORS__MAX_AGE=600
    """

    enabled: bool = Field(default=True, description="Set to ``True`` to enable middleware")
    allow_origins: List[str] = Field(default_factory=list, description="Domains allowed for CORS")
    allow_credentials: bool = Field(
        default=False,
        description="If ``True``, cookies should be supported for cross-origin request",
    )
    allow_methods: List[str] = Field(default=["GET"], description="HTTP Methods allowed for CORS")
    # https://github.com/snok/asgi-correlation-id#cors
    allow_headers: List[str] = Field(
        default=["X-Request-ID", "X-Request-With"],
        description="HTTP headers allowed for CORS",
    )
    expose_headers: List[str] = Field(default=["X-Request-ID"], description="HTTP headers exposed from backend")

    model_config = ConfigDict(extra="allow")
