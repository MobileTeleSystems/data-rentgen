from collections.abc import AsyncGenerator
from random import randint
from typing import AsyncContextManager, Callable

import pytest
import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import User
from tests.test_server.fixtures.factories.base import random_string


def user_factory(**kwargs):
    data = {
        "id": randint(0, 10000000),
        "name": random_string(16),
    }
    data.update(kwargs)
    return User(**data)


@pytest_asyncio.fixture(params=[{}])
async def user(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
) -> AsyncGenerator[User, None]:
    params = request.param
    item = user_factory(**params)
    del item.id
    async with async_session_maker() as async_session:
        async_session.add(item)

        await async_session.commit()
        await async_session.refresh(item)

        async_session.expunge_all()

    yield item

    delete_query = delete(User).where(User.id == item.id)
    async with async_session_maker() as async_session:
        await async_session.execute(delete_query)
        await async_session.commit()
