from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
from random import choice
from typing import AsyncContextManager, Callable

import pytest
import pytest_asyncio
from sqlalchemy import delete
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
    async_session_maker: Callable[[], AsyncContextManager[AsyncSession]],
    job: Job,
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
        async_session.add_all(runs + datasets + operations + interactions)
        await async_session.commit()
        for item in runs + datasets + operations + interactions:
            await async_session.refresh(item)

        async_session.expunge_all()

    yield job, runs, datasets, operations, interactions

    delete_interaction = delete(Interaction).where(
        Interaction.operation_id.in_([operation.id for operation in operations]),
    )
    delete_operation = delete(Operation).where(Operation.id.in_([operation.id for operation in operations]))
    delete_dataset = delete(Dataset).where(Dataset.id.in_([item.id for item in datasets]))
    delete_run = delete(Run).where(Run.id.in_([item.id for item in runs]))
    async with async_session_maker() as async_session:
        await async_session.execute(delete_interaction)
        await async_session.execute(delete_operation)
        await async_session.execute(delete_dataset)
        await async_session.execute(delete_run)
        await async_session.commit()


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
        async_session.add_all([run] + datasets + operations + interactions)

        await async_session.commit()
        for item in [run] + datasets + operations + interactions:
            await async_session.refresh(item)

        async_session.expunge_all()

    yield run, datasets, operations

    delete_interaction = delete(Interaction).where(
        Interaction.operation_id.in_([operation.id for operation in operations]),
    )
    delete_operation = delete(Operation).where(Operation.id.in_([operation.id for operation in operations]))
    delete_dataset = delete(Dataset).where(Dataset.id.in_([item.id for item in datasets]))
    delete_run = delete(Run).where(Run.id == run.id)
    async with async_session_maker() as async_session:
        await async_session.execute(delete_interaction)
        await async_session.execute(delete_operation)
        await async_session.execute(delete_dataset)
        await async_session.execute(delete_run)
        await async_session.commit()


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
            created_at=operation.created_at + timedelta(seconds=1),
            operation_id=operation.id,
            dataset_id=dataset.id,
            type=InteractionType.APPEND,
        )
        for dataset in datasets
    ]

    async with async_session_maker() as async_session:
        async_session.add_all(datasets + [operation] + interactions)

        await async_session.commit()
        for item in datasets + [operation] + interactions:
            await async_session.refresh(item)

        async_session.expunge_all()

    yield operation, datasets

    delete_interaction = delete(Interaction).where(Interaction.operation_id == operation.id)
    delete_operation = delete(Operation).where(Operation.id == operation.id)
    delete_dataset = delete(Dataset).where(Dataset.id.in_([item.id for item in datasets]))
    async with async_session_maker() as async_session:
        await async_session.execute(delete_interaction)
        await async_session.execute(delete_operation)
        await async_session.execute(delete_dataset)
        await async_session.commit()


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
        async_session.add_all([dataset] + operations + interactions)

        await async_session.commit()
        for item in [dataset] + operations + interactions:
            await async_session.refresh(item)

        async_session.expunge_all()

    yield dataset, operations

    delete_interaction = delete(Interaction).where(Interaction.dataset_id == dataset.id)
    delete_dataset = delete(Dataset).where(Dataset.id == dataset.id)
    delete_operation = delete(Operation).where(Operation.id.in_([operation.id for operation in operations]))
    async with async_session_maker() as async_session:
        await async_session.execute(delete_interaction)
        await async_session.execute(delete_dataset)
        await async_session.execute(delete_operation)
        await async_session.commit()
