from random import randint

from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Input
from data_rentgen.db.utils.uuid import extract_timestamp_from_uuid, generate_new_uuid
from tests.test_server.fixtures.factories.schema import create_schema


def input_factory(**kwargs) -> Input:
    created_at = kwargs.pop("created_at", None)
    input_id = generate_new_uuid(created_at)
    data = {
        "id": input_id,
        "created_at": extract_timestamp_from_uuid(input_id),
        "operation_id": generate_new_uuid(),
        "run_id": generate_new_uuid(),
        "job_id": randint(0, 10000000),
        "dataset_id": randint(0, 10000000),
        "schema_id": randint(0, 10000000),
        "num_bytes": randint(0, 10000000),
        "num_rows": randint(0, 10000),
        "num_files": randint(0, 100),
    }
    data.update(kwargs)
    return Input(**data)


async def create_input(
    async_session: AsyncSession,
    input_kwargs: dict | None = None,
    schema_kwargs: dict | None = None,
) -> Input:
    input_kwargs = input_kwargs or {}
    schema = await create_schema(async_session, schema_kwargs)
    input_kwargs.update({"schema_id": schema.id})
    input = input_factory(**input_kwargs)
    async_session.add(input)
    await async_session.commit()
    await async_session.refresh(input)
    return input
