# SPDX-FileCopyrightText: 2023 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest
from fastapi import FastAPI

from arrakis.backend import application_factory
from arrakis.backend.settings import Settings


@pytest.fixture(scope="session")
def test_app(settings: Settings) -> FastAPI:
    return application_factory(settings=settings)
