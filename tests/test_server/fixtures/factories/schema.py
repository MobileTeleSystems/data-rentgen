from __future__ import annotations

from random import randint
from typing import TYPE_CHECKING
from uuid import uuid4

from data_rentgen.db.models import Schema

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


def schema_factory(**kwargs) -> Schema:
    data = {
        "id": randint(0, 10000000),
        "digest": uuid4(),
        "fields": [
            {"name": "dt", "type": "timestamp"},
            {"name": "customer_id", "type": "decimal(20,0)"},
            {"name": "total_spent", "type": "float"},
        ],
    }
    data.update(kwargs)
    return Schema(**data)


async def create_schema(async_session: AsyncSession, schema_kwargs: dict | None = None) -> Schema:
    schema_kwargs = schema_kwargs or {}
    schema = schema_factory(**schema_kwargs)
    del schema.id
    async_session.add(schema)
    await async_session.commit()
    await async_session.refresh(schema)
    return schema
