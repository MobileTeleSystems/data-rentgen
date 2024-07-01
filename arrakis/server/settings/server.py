# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import textwrap

from pydantic import BaseModel, Field

from arrakis.server.settings.application_version import ApplicationVersionSettings
from arrakis.server.settings.cors import CORSSettings
from arrakis.server.settings.monitoring import MonitoringSettings
from arrakis.server.settings.openapi import OpenAPISettings
from arrakis.server.settings.request_id import RequestIDSettings
from arrakis.server.settings.static_files import StaticFilesSettings


class ServerSettings(BaseModel):
    """Arrakis REST API server-specific settings.

    Examples
    --------

    .. code-block:: bash

        ARRAKIS__SERVER__DEBUG=True
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
            :ref:`Enable debug output in responses <configuration-server-debug>`.
            Do not use this on production!
            """,
        ),
    )
    cors: CORSSettings = Field(
        default_factory=CORSSettings,
        description=":ref:`CORS settings <configuration-server-cors>`",
    )
    monitoring: MonitoringSettings = Field(
        default_factory=MonitoringSettings,
        description=":ref:`Monitoring settings <configuration-server-monitoring>`",
    )
    request_id: RequestIDSettings = Field(
        default_factory=RequestIDSettings,
        description=":ref:`RequestID settings <configuration-server-debug>`",
    )
    application_version: ApplicationVersionSettings = Field(
        default_factory=ApplicationVersionSettings,
        description=":ref:`Application version settings <configuration-server-debug>`",
    )
    static_files: StaticFilesSettings = Field(
        default_factory=StaticFilesSettings,
        description=":ref:`Static files settings <configuration-server-static-files>`",
    )
    openapi: OpenAPISettings = Field(
        default_factory=OpenAPISettings,
        description=":ref:`OpenAPI.json settings <configuration-server-openapi>`",
    )
