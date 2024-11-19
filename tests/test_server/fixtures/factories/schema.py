from random import randint
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Schema


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


async def create_schema(session: AsyncSession, schema_kwargs: dict | None = None) -> Schema:
    schema_kwargs = schema_kwargs or {}
    schema = schema_factory(**schema_kwargs)
    del schema.id
    session.add(schema)
    await session.commit()
    await session.refresh(schema)
    return schema
