# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from fastapi import FastAPI

from arrakis.backend.middlewares.application_version import (
    apply_application_version_middleware,
)
from arrakis.backend.middlewares.cors import apply_cors_middleware
from arrakis.backend.middlewares.logging import setup_logging
from arrakis.backend.middlewares.monitoring import apply_monitoring_metrics_middleware
from arrakis.backend.middlewares.openapi import apply_openapi_middleware
from arrakis.backend.middlewares.request_id import apply_request_id_middleware
from arrakis.backend.middlewares.static_files import apply_static_files
from arrakis.backend.settings import Settings


def apply_middlewares(
    application: FastAPI,
    settings: Settings,
) -> FastAPI:
    """Add middlewares to the application."""

    if settings.server.logging.setup:
        setup_logging(settings.server.logging.get_log_config_path())

    apply_cors_middleware(application, settings.server.cors)
    apply_monitoring_metrics_middleware(application, settings.server.monitoring)
    apply_request_id_middleware(application, settings.server.request_id)
    apply_application_version_middleware(application, settings.server.application_version)
    apply_openapi_middleware(application, settings.server.openapi)
    apply_static_files(application, settings.server.static_files)

    return application
