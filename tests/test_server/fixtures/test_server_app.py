from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from data_rentgen.server import application_factory

if TYPE_CHECKING:
    from fastapi import FastAPI

    from data_rentgen.server.settings import ServerApplicationSettings


@pytest.fixture(scope="session")
def test_server_app(server_app_settings: ServerApplicationSettings) -> FastAPI:
    return application_factory(settings=server_app_settings)
