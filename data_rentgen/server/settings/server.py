# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import textwrap

from pydantic import BaseModel, Field

from data_rentgen.server.settings.application_version import ApplicationVersionSettings
from data_rentgen.server.settings.cors import CORSSettings
from data_rentgen.server.settings.monitoring import MonitoringSettings
from data_rentgen.server.settings.openapi import OpenAPISettings
from data_rentgen.server.settings.request_id import RequestIDSettings
from data_rentgen.server.settings.session import SessionSettings
from data_rentgen.server.settings.static_files import StaticFilesSettings


class ServerSettings(BaseModel):
    """Data.Rentgen REST API server-specific settings.

    Examples
    --------

    .. code-block:: bash

        DATA_RENTGEN__SERVER__DEBUG=True
        DATA_RENTGEN__SERVER__MONITORING__ENABLED=True
        DATA_RENTGEN__SERVER__CORS__ENABLED=True
        DATA_RENTGEN__SERVER__REQUEST_ID__ENABLED=True
        DATA_RENTGEN__SERVER__OPENAPI__ENABLED=True
        DATA_RENTGEN__SERVER__OPENAPI__SWAGGER__ENABLED=True
        DATA_RENTGEN__SERVER__OPENAPI__REDOC__ENABLED=True
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
    session: SessionSettings = Field(
        default_factory=SessionSettings,  # type: ignore[arg-type]
        description=":ref:`Session settings <configuration-server-session>`",
    )
