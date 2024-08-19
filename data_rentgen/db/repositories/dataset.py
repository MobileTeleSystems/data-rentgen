# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime

from sqlalchemy import and_, select
from sqlalchemy.orm import selectinload

from data_rentgen.db.models import Dataset, Interaction, Location, Operation
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import (
    DatasetDTO,
    DatasetNodeDTO,
    OperationDatasetDTO,
    PaginationDTO,
)


class DatasetRepository(Repository[Dataset]):
    async def create_or_update(self, dataset: DatasetDTO, location_id: int) -> Dataset:
        result = await self._get(location_id, dataset.name)

        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock(location_id, dataset.name)
            result = await self._get(location_id, dataset.name)

        if not result:
            return await self._create(dataset, location_id)
        return await self._update(result, dataset)

    async def paginate(self, page: int, page_size: int, dataset_ids: list[int]) -> PaginationDTO[Dataset]:
        query = select(Dataset).options(selectinload(Dataset.location).selectinload(Location.addresses))
        if dataset_ids:
            query = query.where(Dataset.id.in_(dataset_ids))
        return await self._paginate_by_query(order_by=[Dataset.name], page=page, page_size=page_size, query=query)

    async def get_dataset_operations(
        self,
        dataset_id: int,
        type: list[str],
        since: datetime,
        until: datetime | None,
    ) -> list[OperationDatasetDTO]:
        # filter by time on interaction.
        filter = [Interaction.created_at >= since, Dataset.id == dataset_id, Interaction.type.in_(type)]
        if until:
            filter.append(Interaction.created_at <= since)
        query = (
            select(Operation.id, Dataset.id, Interaction.type)
            .join(Interaction, Dataset.id == Interaction.dataset_id)
            .join(Operation, Interaction.operation_id == Operation.id)
            .where(and_(*filter))
        )
        result = await self._session.execute(query)
        return [OperationDatasetDTO(*row) for row in result.all()]

    async def get_node_info(self, dataset_id: int):
        query = select(Dataset.id, Dataset.name).where(Dataset.id == dataset_id)
        result = await self._session.execute(query)
        return DatasetNodeDTO(*result.one())

    async def _get(self, location_id: int, name: str) -> Dataset | None:
        statement = select(Dataset).where(Dataset.location_id == location_id, Dataset.name == name)
        return await self._session.scalar(statement)

    async def _create(self, dataset: DatasetDTO, location_id: int) -> Dataset:
        result = Dataset(location_id=location_id, name=dataset.name, format=dataset.format)
        self._session.add(result)
        await self._session.flush([result])
        return result

    async def _update(self, existing: Dataset, new: DatasetDTO) -> Dataset:
        if new.format:
            existing.format = new.format
            await self._session.flush([existing])
        return existing
