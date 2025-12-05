# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware

from data_rentgen.server.settings.session import SessionSettings


def apply_session_middleware(app: FastAPI, settings: SessionSettings) -> FastAPI:
    """Add SessionMiddleware middleware to the application."""
    if not settings.enabled:
        return app

    settings_dict = settings.model_dump(exclude={"secret_key", "enabled"})
    settings_dict["secret_key"] = settings.secret_key.get_secret_value()  # type: ignore[union-attr]

    app.add_middleware(SessionMiddleware, **settings_dict)
    return app
