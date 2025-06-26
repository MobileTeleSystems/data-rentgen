from __future__ import annotations

from datetime import timedelta
from random import choice, randint
from typing import TYPE_CHECKING, Callable

import pytest_asyncio

from data_rentgen.db.models import Operation, OperationStatus, OperationType, Run
from data_rentgen.utils.uuid import extract_timestamp_from_uuid, generate_new_uuid
from tests.test_server.fixtures.factories.base import random_datetime, random_string
from tests.test_server.fixtures.factories.sql_query import create_sql_query
from tests.test_server.utils.delete import clean_db

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from contextlib import AbstractAsyncContextManager

    import pytest
    from sqlalchemy.ext.asyncio import AsyncSession


def operation_factory(**kwargs):
    created_at = kwargs.pop("created_at", None)
    operation_id = generate_new_uuid(created_at)
    data = {
        "created_at": extract_timestamp_from_uuid(operation_id),
        "id": operation_id,
        "run_id": generate_new_uuid(),
        "name": random_string(),
        "status": choice(list(OperationStatus)),
        "type": choice(list(OperationType)),
        "position": randint(1, 10),
        "group": random_string(32),
        "description": random_string(32),
        "started_at": random_datetime(),
        "ended_at": random_datetime(),
    }
    data.update(kwargs)
    return Operation(**data)


async def create_operation(
    async_session: AsyncSession,
    operation_kwargs: dict | None = None,
) -> Operation:
    operation_kwargs = operation_kwargs or {}
    operation = operation_factory(**operation_kwargs)
    async_session.add(operation)
    await async_session.commit()
    await async_session.refresh(operation)

    return operation


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
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    run: Run,
) -> AsyncGenerator[Operation, None]:
    params = request.param

    async with async_session_maker() as async_session:
        operation = await create_operation(
            async_session=async_session,
            operation_kwargs={
                "run_id": run.id,
                "created_at": run.created_at + timedelta(seconds=1),
                **params,
            },
        )

    yield operation

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture(params=[(10, {})])
async def operations(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    runs: list[Run],
) -> AsyncGenerator[list[Operation], None]:
    size, params = request.param
    items = []

    async with async_session_maker() as async_session:
        for index in range(size):
            items.append(  # noqa: PERF401
                await create_operation(
                    async_session=async_session,
                    operation_kwargs={
                        "run_id": runs[index].id,
                        "created_at": runs[index].created_at + timedelta(seconds=index),
                        **params,
                    },
                ),
            )

        async_session.expunge_all()

    yield items

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture(params=[(10, {})])
async def operations_with_same_run(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    run: Run,
) -> AsyncGenerator[list[Operation], None]:
    size, params = request.param

    async with async_session_maker() as async_session:
        items = [
            await create_operation(
                async_session=async_session,
                operation_kwargs={
                    "run_id": run.id,
                    "created_at": run.created_at + timedelta(seconds=index),
                    **params,
                },
            )
            for index in range(size)
        ]

        async_session.expunge_all()

    yield items

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture(params=[{}])
async def operation_with_sql_query(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    run: Run,
) -> AsyncGenerator[Operation, None]:
    params = request.param

    async with async_session_maker() as async_session:
        sql_query = await create_sql_query(
            async_session=async_session,
            sql_query_kwargs={"query": "select id, name\nfrom schema.table\nwhere id = 1"},
        )
        operation = await create_operation(
            async_session=async_session,
            operation_kwargs={
                "run_id": run.id,
                "created_at": run.created_at + timedelta(seconds=1),
                "sql_query_id": sql_query.id,
                **params,
            },
        )

    yield operation

    async with async_session_maker() as async_session:
        await clean_db(async_session)
