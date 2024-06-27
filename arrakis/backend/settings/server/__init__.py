# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import textwrap

from pydantic import BaseModel, Field

from arrakis.backend.settings.server.application_version import (
    ApplicationVersionSettings,
)
from arrakis.backend.settings.server.cors import CORSSettings
from arrakis.backend.settings.server.log import LoggingSettings
from arrakis.backend.settings.server.monitoring import MonitoringSettings
from arrakis.backend.settings.server.openapi import OpenAPISettings
from arrakis.backend.settings.server.request_id import RequestIDSettings
from arrakis.backend.settings.server.static_files import StaticFilesSettings


class ServerSettings(BaseModel):
    """Backend server settings.

    Examples
    --------

    .. code-block:: bash

        ARRAKIS__SERVER__DEBUG=True
        ARRAKIS__SERVER__LOGGING__PRESET=colored
        ARRAKIS__SERVER__MONITORING__ENABLED=True
        ARRAKIS__SERVER__CORS__ENABLED=True
        ARRAKIS__SERVER__REQUEST_ID__ENABLED=True
        ARRAKIS__SERVER__OPENAPI__ENABLED=True
        ARRAKIS__SERVER__OPENAPI__SWAGGER__ENABLED=True
        ARRAKIS__SERVER__OPENAPI__REDOC__ENABLED=True
    """

    debug: bool = Field(
        default=False,
        description=textwrap.dedent(
            """
            :ref:`Enable debug output in responses <backend-configuration-debug>`.
            Do not use this on production!
            """,
        ),
    )
    logging: LoggingSettings = Field(
        default_factory=LoggingSettings,
        description=":ref:`Logging settings <backend-configuration-logging>`",
    )
    cors: CORSSettings = Field(
        default_factory=CORSSettings,
        description=":ref:`CORS settings <backend-configuration-cors>`",
    )
    monitoring: MonitoringSettings = Field(
        default_factory=MonitoringSettings,
        description=":ref:`Monitoring settings <backend-configuration-monitoring>`",
    )
    request_id: RequestIDSettings = Field(
        default_factory=RequestIDSettings,
        description=":ref:`RequestID settings <backend-configuration-debug>`",
    )
    application_version: ApplicationVersionSettings = Field(
        default_factory=ApplicationVersionSettings,
        description=":ref:`Application version settings <backend-configuration-debug>`",
    )
    static_files: StaticFilesSettings = Field(
        default_factory=StaticFilesSettings,
        description=":ref:`Static files settings <backend-configuration-static-files>`",
    )
    openapi: OpenAPISettings = Field(
        default_factory=OpenAPISettings,
        description=":ref:`OpenAPI.json settings <backend-configuration-openapi>`",
    )
