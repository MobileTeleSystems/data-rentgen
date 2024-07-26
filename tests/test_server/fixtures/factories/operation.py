from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
from random import choice, randint
from typing import AsyncContextManager, Callable

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Operation, OperationType, Run, Status
from data_rentgen.db.utils.uuid import extract_timestamp_from_uuid, generate_new_uuid
from tests.test_server.fixtures.factories.base import random_datetime, random_string


def operation_factory_minimal(**kwargs):
    if kwargs.get("created_at_dttm"):
        operation_id = generate_new_uuid(kwargs.pop("created_at_dttm"))
    else:
        operation_id = generate_new_uuid()
    data = {
        "created_at": extract_timestamp_from_uuid(operation_id),
        "id": operation_id,
        "run_id": generate_new_uuid(),
        "name": random_string(),
    }
    data.update(kwargs)
    return Operation(**data)


def operation_factory(**kwargs):
    if kwargs.get("created_at_dttm"):
        operation_id = generate_new_uuid(kwargs.pop("created_at_dttm"))
    else:
        operation_id = generate_new_uuid()
    data = {
        "created_at": extract_timestamp_from_uuid(operation_id),
        "id": operation_id,
        "run_id": generate_new_uuid(),
        "name": random_string(),
        "status": choice(list(Status)),
        "type": choice(list(OperationType)),
        "position": randint(1, 10),
        "description": random_string(32),
        "started_at": random_datetime(),
        "ended_at": random_datetime(),
    }
    data.update(kwargs)
    return Operation(**data)


@pytest_asyncio.fixture(params=[{}])
async def new_operation(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[Operation, None]:
    params = request.param
    item = operation_factory_minimal(**params)

    yield item


@pytest_asyncio.fixture(params=[{}])
async def operation(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    run: Run,
) -> AsyncGenerator[Operation, None]:
    params = request.param
    item = operation_factory_minimal(run_id=run.id, **params)

    async with async_session_maker() as async_session:
        async_session.add(item)
        # this is not required for backend tests, but needed by client tests
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        await async_session.refresh(item)
        async_session.expunge(item)

    yield item


@pytest_asyncio.fixture(params=[(5, {})])
async def operations(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    runs: list[Run],
) -> AsyncGenerator[list[Operation], None]:
    size, params = request.param
    started_at = datetime.now()
    items = [
        operation_factory_minimal(
            run_id=choice(runs).id,
            created_at_dttm=started_at + timedelta(seconds=0.1 * i),
            **params,
        )
        for i in range(size)
    ]
    items.sort(key=lambda x: (x.run_id, x.id))

    async with async_session_maker() as async_session:
        for item in items:
            async_session.add(item)
        # this is not required for backend tests, but needed by client tests
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        for item in items:
            await async_session.refresh(item)
            async_session.expunge(item)

    yield items


@pytest_asyncio.fixture(params=[(5, {})])
async def operations_with_same_run(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    run: Run,
) -> AsyncGenerator[list[Operation], None]:
    size, params = request.param
    started_at = datetime.now()
    items = [
        operation_factory_minimal(
            run_id=run.id,
            created_at_dttm=started_at + timedelta(seconds=s),
            **params,
        )
        for s in range(size)
    ]

    async with async_session_maker() as async_session:
        for item in items:
            async_session.add(item)
        # this is not required for backend tests, but needed by client tests
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        for item in items:
            await async_session.refresh(item)
            async_session.expunge(item)

    yield items


@pytest_asyncio.fixture(params=[{}])
async def operation_with_all_fields(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    run: Run,
) -> AsyncGenerator[Operation, None]:
    params = request.param
    item = operation_factory(run_id=run.id, **params)

    async with async_session_maker() as async_session:
        async_session.add(item)
        # this is not required for backend tests, but needed by client tests
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        await async_session.refresh(item)
        async_session.expunge(item)

    yield item
