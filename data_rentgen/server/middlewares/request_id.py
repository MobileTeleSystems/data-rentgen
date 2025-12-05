# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from asgi_correlation_id import CorrelationIdMiddleware
from fastapi import FastAPI
from uuid6 import uuid8

from data_rentgen.server.settings.server import RequestIDSettings


def apply_request_id_middleware(app: FastAPI, settings: RequestIDSettings) -> FastAPI:
    """Add X-Request-ID middleware to the application."""
    if not settings.enabled:
        return app

    app.add_middleware(
        CorrelationIdMiddleware,
        generator=lambda: uuid8().hex,
        validator=None,
        **settings.model_dump(exclude={"enabled"}),
    )
    return app
