# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker


def sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )


def create_session_factory(engine: AsyncEngine):
    Session = sessionmaker(engine)  # noqa: N806

    async def wrapper() -> AsyncGenerator[AsyncSession, None]:
        async with Session.begin() as session:
            yield session

    return wrapper
