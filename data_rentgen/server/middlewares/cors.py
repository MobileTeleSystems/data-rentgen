# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from data_rentgen.server.settings.server import CORSSettings


def apply_cors_middleware(app: FastAPI, settings: CORSSettings) -> FastAPI:
    """Add CORS middleware to the application."""
    if not settings:
        return app

    app.add_middleware(
        CORSMiddleware,
        **settings.model_dump(exclude={"enabled"}),
    )
    return app
