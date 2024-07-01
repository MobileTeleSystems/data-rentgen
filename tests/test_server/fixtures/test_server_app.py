# SPDX-FileCopyrightText: 2023 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest
from fastapi import FastAPI

from arrakis.server import application_factory
from arrakis.server.settings import ServerApplicationSettings


@pytest.fixture(scope="session")
def test_server_app(server_app_settings: ServerApplicationSettings) -> FastAPI:
    return application_factory(settings=server_app_settings)
