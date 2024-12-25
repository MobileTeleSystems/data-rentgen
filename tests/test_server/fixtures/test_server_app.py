from __future__ import annotations

import pytest
from fastapi import FastAPI

from data_rentgen.server import application_factory
from data_rentgen.server.settings import ServerApplicationSettings


@pytest.fixture(scope="session")
def test_server_app(server_app_settings: ServerApplicationSettings) -> FastAPI:
    return application_factory(settings=server_app_settings)
