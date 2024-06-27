# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from arrakis.backend.settings.server.static_files import StaticFilesSettings


def apply_static_files(app: FastAPI, settings: StaticFilesSettings) -> FastAPI:
    """Add static files serving middleware to the application."""
    if not settings.enabled:
        return app

    # https://fastapi.tiangolo.com/how-to/custom-docs-ui-assets/#serve-the-static-files
    app.mount("/static", StaticFiles(directory=settings.directory), name="static")
    return app
