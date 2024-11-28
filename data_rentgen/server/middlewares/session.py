# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware

from data_rentgen.server.settings.session import SessionSettings


def apply_session_middleware(app: FastAPI, settings: SessionSettings) -> FastAPI:
    """Add SessionMiddleware middleware to the application."""

    app.add_middleware(
        SessionMiddleware,
        **settings.dict(),
    )
    return app
