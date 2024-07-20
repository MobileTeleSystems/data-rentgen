from collections.abc import AsyncGenerator
from random import randint
from typing import AsyncContextManager, Callable

import pytest
import pytest_asyncio
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

    async with async_session_maker() as async_session:
        async_session.add(item)
        # this is not required for backend tests, but needed by client tests
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        await async_session.refresh(item)
        async_session.expunge(item)

    yield item
