from __future__ import annotations

from random import randint
from typing import TYPE_CHECKING
from uuid import uuid4

from data_rentgen.db.models.sql_query import SQLQuery
from tests.test_server.fixtures.factories.base import random_string

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


def sql_query_factory(**kwargs):
    data = {
        "id": randint(0, 10000000),
        "fingerprint": uuid4(),
        "query": random_string(20),
    }
    data.update(kwargs)
    return SQLQuery(**data)


async def create_sql_query(
    async_session: AsyncSession,
    sql_query_kwargs: dict | None = None,
) -> SQLQuery:
    sql_query_kwargs = sql_query_kwargs or {}
    sql_query = sql_query_factory(**sql_query_kwargs)
    del sql_query.id
    async_session.add(sql_query)
    await async_session.commit()
    await async_session.refresh(sql_query)
    return sql_query
