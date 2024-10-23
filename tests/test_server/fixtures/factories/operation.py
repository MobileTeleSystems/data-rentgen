from collections.abc import AsyncGenerator
from datetime import timedelta
from random import choice, randint
from typing import AsyncContextManager, Callable

import pytest
import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Operation, OperationType, Run, Status
from data_rentgen.db.utils.uuid import extract_timestamp_from_uuid, generate_new_uuid
from tests.test_server.fixtures.factories.base import random_datetime, random_string


def operation_factory(**kwargs):
    created_at = kwargs.pop("created_at", None)
    operation_id = generate_new_uuid(created_at)
    data = {
        "created_at": extract_timestamp_from_uuid(operation_id),
        "id": operation_id,
        "run_id": generate_new_uuid(),
        "name": random_string(),
        "status": choice(list(Status)),
        "type": choice(list(OperationType)),
        "position": randint(1, 10),
        "group": random_string(32),
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
    item = operation_factory(**params)

    yield item


@pytest_asyncio.fixture(params=[{}])
async def operation(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    run: Run,
) -> AsyncGenerator[Operation, None]:
    params = request.param
    item = operation_factory(run_id=run.id, created_at=run.created_at + timedelta(seconds=1), **params)

    async with async_session_maker() as async_session:
        async_session.add(item)

        await async_session.commit()
        await async_session.refresh(item)

        async_session.expunge_all()

    yield item

    delete_query = delete(Operation).where(Operation.id == item.id)
    async with async_session_maker() as async_session:
        await async_session.execute(delete_query)
        await async_session.commit()


@pytest_asyncio.fixture(params=[(10, {})])
async def operations(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    runs: list[Run],
) -> AsyncGenerator[list[Operation], None]:
    size, params = request.param
    items = []
    for index in range(size):
        run = runs[index]
        items.append(
            operation_factory(
                run_id=run.id,
                created_at=run.created_at + timedelta(seconds=index),
                **params,
            ),
        )

    async with async_session_maker() as async_session:
        async_session.add_all(items)

        await async_session.commit()
        for item in items:
            await async_session.refresh(item)

        async_session.expunge_all()

    yield items

    delete_query = delete(Operation).where(Operation.id.in_([item.id for item in items]))
    async with async_session_maker() as async_session:
        await async_session.execute(delete_query)
        await async_session.commit()


@pytest_asyncio.fixture(params=[(10, {})])
async def operations_with_same_run(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    run: Run,
) -> AsyncGenerator[list[Operation], None]:
    size, params = request.param
    items = [
        operation_factory(
            run_id=run.id,
            created_at=run.created_at + timedelta(seconds=index),
            **params,
        )
        for index in range(size)
    ]

    async with async_session_maker() as async_session:
        async_session.add_all(items)

        await async_session.commit()
        for item in items:
            await async_session.refresh(item)

        async_session.expunge_all()

    yield items

    delete_query = delete(Operation).where(Operation.id.in_([item.id for item in items]))
    async with async_session_maker() as async_session:
        await async_session.execute(delete_query)
        await async_session.commit()
