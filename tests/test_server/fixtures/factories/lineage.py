from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
from random import choice
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
from tests.test_server.fixtures.factories.dataset import dataset_factory
from tests.test_server.fixtures.factories.interaction import interaction_factory
from tests.test_server.fixtures.factories.operation import operation_factory
from tests.test_server.fixtures.factories.run import run_factory


@pytest_asyncio.fixture()
async def lineage(
    job: Job,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    addresses: list[Address],
) -> AsyncGenerator[tuple[Job, list[Run], list[Dataset], list[Operation], list[Interaction]], None]:
    datasets = [dataset_factory(location_id=choice(addresses).location_id) for _ in range(4)]
    started_at = datetime.now()
    runs = [run_factory(job_id=job.id, created_at=started_at + timedelta(seconds=s)) for s in range(2)]

    operations = [
        operation_factory(run_id=run.id, created_at=run.created_at + timedelta(seconds=s))
        for run in runs
        for s in range(2)
    ]  # Two operations for each run
    read = InteractionType.READ
    append = InteractionType.APPEND
    interactions = [
        interaction_factory(
            created_at=operation.created_at,
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


@pytest_asyncio.fixture(params=[(3, {})])
async def run_lineage(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    addresses: list[Address],
):
    size, params = request.param
    started_at = datetime.now()
    run = run_factory(created_at=started_at)
    datasets = [dataset_factory(location_id=choice(addresses).location_id) for _ in range(size)]
    operations = [
        operation_factory(run_id=run.id, created_at=run.created_at + timedelta(seconds=s)) for s in range(size)
    ]

    interactions = [
        interaction_factory(
            created_at=operation.created_at,
            operation_id=operation.id,
            dataset_id=dataset.id,
            type=InteractionType.APPEND,
        )
        for operation, dataset in zip(operations, datasets)
    ]

    async with async_session_maker() as async_session:
        for item in [run] + datasets + operations + interactions:
            async_session.add(item)
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache

        for item in [run] + datasets + operations + interactions:
            await async_session.refresh(item)
            async_session.expunge(item)

    yield run, datasets, operations


@pytest_asyncio.fixture(params=[(3, {})])
async def operation_to_datasets_lineage(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    addresses: list[Address],
):
    size, params = request.param
    started_at = datetime.now()
    datasets = [dataset_factory(location_id=choice(addresses).location_id) for _ in range(size)]
    operation = operation_factory(created_at=started_at, **params)

    interactions = [
        interaction_factory(
            created_at=started_at + timedelta(seconds=1),
            operation_id=operation.id,
            dataset_id=dataset.id,
            type=InteractionType.APPEND,
        )
        for dataset in datasets
    ]

    async with async_session_maker() as async_session:
        for item in datasets + [operation] + interactions:
            async_session.add(item)
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        for item in datasets + [operation] + interactions:
            await async_session.refresh(item)
            async_session.expunge(item)

    yield operation, datasets


@pytest_asyncio.fixture(params=[(3, {})])
async def operations_to_dataset_lineage(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    address: Address,
):
    size, params = request.param
    started_at = datetime.now()
    dataset = dataset_factory(location_id=address.location_id)
    operations = [operation_factory(created_at=started_at, **params) for _ in range(size)]

    interactions = [
        interaction_factory(
            created_at=started_at + timedelta(seconds=index),
            operation_id=operation.id,
            dataset_id=dataset.id,
            type=InteractionType.APPEND,
        )
        for index, operation in enumerate(operations)
    ]

    async with async_session_maker() as async_session:
        for item in [dataset] + operations + interactions:
            async_session.add(item)
        await async_session.commit()

        # remove current object from async_session. this is required to compare object against new state fetched
        # from database, and also to remove it from cache
        for item in [dataset] + operations + interactions:
            await async_session.refresh(item)
            async_session.expunge(item)

    yield dataset, operations
