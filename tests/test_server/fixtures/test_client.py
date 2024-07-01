# SPDX-FileCopyrightText: 2023 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient


@pytest_asyncio.fixture(scope="session")
async def test_client(test_server_app: FastAPI) -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(
        transport=ASGITransport(app=test_server_app),
        base_url="http://arrakis",
    ) as result:
        yield result