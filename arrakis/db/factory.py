# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_engine_from_config,
    async_sessionmaker,
)

from arrakis.db.settings import DatabaseSettings


def create_session_factory(settings: DatabaseSettings) -> async_sessionmaker[AsyncSession]:
    engine = async_engine_from_config(settings.model_dump(), prefix="")
    return async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
