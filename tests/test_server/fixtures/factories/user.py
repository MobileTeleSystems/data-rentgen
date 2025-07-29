from __future__ import annotations

from random import randint
from typing import TYPE_CHECKING

import pytest_asyncio
from sqlalchemy import delete

from data_rentgen.db.models import User
from tests.test_server.fixtures.factories.base import random_string

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable
    from contextlib import AbstractAsyncContextManager

    import pytest
    from sqlalchemy.ext.asyncio import AsyncSession


def user_factory(**kwargs):
    data = {
        "id": randint(0, 10000000),
        "name": random_string(16),
    }
    data.update(kwargs)
    return User(**data)


async def create_user(
    async_session: AsyncSession,
    user_kwargs: dict | None = None,
) -> User:
    user_kwargs = user_kwargs or {}
    user = user_factory(**user_kwargs)
    del user.id
    async_session.add(user)
    await async_session.commit()
    await async_session.refresh(user)

    return user


@pytest_asyncio.fixture(params=[{}])
async def new_user(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[User, None]:
    params = request.param
    user = user_factory(**params)

    yield user


@pytest_asyncio.fixture(params=[{}])
async def user(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
) -> AsyncGenerator[User, None]:
    params = request.param
    async with async_session_maker() as async_session:
        item = await create_user(async_session, params)

        await async_session.commit()
        await async_session.refresh(item)

        async_session.expunge_all()

    yield item

    delete_query = delete(User).where(User.id == item.id)
    async with async_session_maker() as async_session:
        await async_session.execute(delete_query)
        await async_session.commit()
