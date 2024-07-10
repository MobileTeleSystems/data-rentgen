# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession

import data_rentgen
from data_rentgen.db.factory import create_session_factory
from data_rentgen.logging.setup_logging import setup_logging
from data_rentgen.server.api.handlers import apply_exception_handlers
from data_rentgen.server.api.router import api_router
from data_rentgen.server.middlewares import apply_middlewares
from data_rentgen.server.settings import ServerApplicationSettings


def application_factory(settings: ServerApplicationSettings) -> FastAPI:
    application = FastAPI(
        title="Data.Rentgen",
        description="Data.Rentgen is a nextgen DataLineage service",
        version=data_rentgen.__version__,
        debug=settings.server.debug,
        # will be set up by middlewares
        openapi_url=None,
        docs_url=None,
        redoc_url=None,
    )

    application.state.settings = settings
    application.include_router(api_router)

    apply_exception_handlers(application)
    apply_middlewares(application, settings.server)

    application.dependency_overrides.update(
        {
            ServerApplicationSettings: lambda: settings,
            AsyncSession: create_session_factory(settings.database),  # type: ignore[dict-item]
        },
    )
    return application


def get_application():
    settings = ServerApplicationSettings()
    setup_logging(settings.logging)
    return application_factory(settings=settings)
