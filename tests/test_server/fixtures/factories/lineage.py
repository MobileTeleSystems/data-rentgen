from collections.abc import AsyncGenerator
from random import choice, randint
from typing import AsyncContextManager, Callable

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import (
    Address,
    Dataset,
    Interaction,
    InteractionType,
    Job,
    Operation,
    Run,
)
from data_rentgen.db.utils.uuid import extract_timestamp_from_uuid, generate_new_uuid
from tests.test_server.fixtures.factories import (
    dataset_factory,
    operation_factory_minimal,
    run_factory_minimal,
)


def interaction_factory_minimal(**kwargs) -> Interaction:
    if kwargs.get("created_at_dttm"):
        interaction_id = generate_new_uuid(kwargs.pop("created_at_dttm"))
    else:
        interaction_id = generate_new_uuid()
    data = {
        "id": interaction_id,
        "created_at": extract_timestamp_from_uuid(interaction_id),
        "operation_id": generate_new_uuid(),
        "dataset_id": randint(0, 10000000),
    }
    data.update(kwargs)
    return Interaction(**data)


@pytest_asyncio.fixture()
async def lineage(
    job: Job,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    addresses: list[Address],
) -> AsyncGenerator[tuple[Job, list[Run], list[Dataset], list[Operation], list[Interaction]], None]:
    datasets = [dataset_factory(location_id=choice(addresses).location_id) for _ in range(4)]
    runs = [run_factory_minimal(job_id=job.id) for _ in range(2)]

    operations = [
        operation_factory_minimal(run_id=run.id) for run in runs for _ in range(2)  # Two operations for each run
    ]
    read = InteractionType.READ
    append = InteractionType.APPEND
    interactions = [
        interaction_factory_minimal(
            operation_id=operation.id,
            dataset_id=dataset.id,
            type=inter_type,
        )
        for operation, dataset, inter_type in zip(operations, datasets, [read, append, read, append])
    ]
    async with async_session_maker() as async_session:
        for item in runs + datasets + operations + interactions:
            async_session.add(item)
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache

        for item in runs + datasets + operations + interactions:
            await async_session.refresh(item)
            async_session.expunge(item)

    yield job, runs, datasets, operations, interactions
