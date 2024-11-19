from random import choice, randint

from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Output, OutputType
from data_rentgen.db.utils.uuid import extract_timestamp_from_uuid, generate_new_uuid
from tests.test_server.fixtures.factories.schema import create_schema


def output_factory(**kwargs) -> Output:
    created_at = kwargs.pop("created_at", None)
    output_id = generate_new_uuid(created_at)
    data = {
        "id": output_id,
        "created_at": extract_timestamp_from_uuid(output_id),
        "operation_id": generate_new_uuid(),
        "run_id": generate_new_uuid(),
        "job_id": randint(0, 10000000),
        "dataset_id": randint(0, 10000000),
        "type": choice(list(OutputType)),
        "schema_id": randint(0, 10000000),
        "num_bytes": randint(0, 10000000),
        "num_rows": randint(0, 10000),
        "num_files": randint(0, 100),
    }
    data.update(kwargs)
    return Output(**data)


async def create_output(
    async_session: AsyncSession,
    output_kwargs: dict | None = None,
    schema_kwargs: dict | None = None,
) -> Output:
    output_kwargs = output_kwargs or {}
    schema = await create_schema(async_session, schema_kwargs)
    output_kwargs.update({"schema_id": schema.id})
    output = output_factory(**output_kwargs)
    async_session.add(output)
    await async_session.commit()
    await async_session.refresh(output)
    return output
