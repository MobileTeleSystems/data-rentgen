# SPDX-FileCopyrightText: 2023 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from data_rentgen.db.models import Base

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine


@pytest_asyncio.fixture(scope="session")
async def async_session_maker(async_engine: AsyncEngine):
    yield async_sessionmaker(
        bind=async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )


@pytest_asyncio.fixture()
async def async_session(async_session_maker: async_sessionmaker[AsyncSession]):
    session: AsyncSession = async_session_maker()

    # start each test on fresh database
    # TODO: Refactoring: change setup in this fixture. Now if you use it in test it's remove all data from db.
    for table in reversed(Base.metadata.sorted_tables):
        await session.execute(delete(table))
    await session.commit()

    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()
