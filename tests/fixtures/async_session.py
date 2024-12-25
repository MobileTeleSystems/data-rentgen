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

    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()

        # cleanup everything the test created
        async with async_session_maker() as another_session:
            for table in reversed(Base.metadata.sorted_tables):
                await another_session.execute(delete(table))
            await another_session.commit()
