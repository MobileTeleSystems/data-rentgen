# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import Type

from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_engine_from_config

import arrakis
from arrakis.backend.api.handlers import (
    application_exception_handler,
    http_exception_handler,
    unknown_exception_handler,
    validation_exception_handler,
)
from arrakis.backend.api.router import api_router
from arrakis.backend.db.factory import create_session_factory
from arrakis.backend.middlewares import apply_middlewares
from arrakis.backend.settings import Settings
from arrakis.commons.exceptions import ApplicationError


def application_factory(settings: Settings) -> FastAPI:
    application = FastAPI(
        title="Arrakis",
        description="Arrakis is a nextgen DataLineage service",
        version=arrakis.__version__,
        debug=settings.server.debug,
        # will be set up by middlewares
        openapi_url=None,
        docs_url=None,
        redoc_url=None,
    )

    application.state.settings = settings
    application.include_router(api_router)

    application.add_exception_handler(ApplicationError, application_exception_handler)  # type: ignore[arg-type]
    application.add_exception_handler(
        RequestValidationError,
        validation_exception_handler,  # type: ignore[arg-type]
    )
    application.add_exception_handler(HTTPException, http_exception_handler)  # type: ignore[arg-type]
    application.add_exception_handler(Exception, unknown_exception_handler)

    engine = async_engine_from_config(settings.database.model_dump(), prefix="")
    session_factory = create_session_factory(engine)

    application.dependency_overrides.update(
        {
            Settings: lambda: settings,
            AsyncSession: session_factory,  # type: ignore[dict-item]
        },
    )

    apply_middlewares(application, settings)
    return application


def get_application():
    settings = Settings()
    return application_factory(settings=settings)
