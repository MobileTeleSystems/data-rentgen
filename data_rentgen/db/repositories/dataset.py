# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Sequence

from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from data_rentgen.db.models import Dataset, Location
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import DatasetDTO, PaginationDTO


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

    async def get_by_id(self, dataset_id: int) -> Dataset | None:
        query = (
            select(Dataset)
            .where(Dataset.id == dataset_id)
            .options(selectinload(Dataset.location).selectinload(Location.addresses))
        )
        return await self._session.scalar(query)

    async def get_by_ids(self, dataset_ids: list[int]) -> Sequence[Dataset]:
        query = (
            select(Dataset)
            .where(Dataset.id.in_(dataset_ids))
            .options(selectinload(Dataset.location).selectinload(Location.addresses))
        )
        result = await self._session.scalars(query)
        return result.all()

    async def search(self, search_query: str, page: int, page_size: int) -> PaginationDTO[Dataset]:
        ts_query = func.plainto_tsquery("english", func.translate(search_query, "/.", "  "))
        query = (
            select(Dataset)
            .where(Dataset.search_vector.op("@@")(ts_query))
            .options(selectinload(Dataset.location).selectinload(Location.addresses))
            .limit(page_size)
            .offset((page - 1) * page_size)
        )
        order_by = [func.ts_rank(Dataset.search_vector, ts_query).desc()]
        return await self._paginate_by_query(order_by=order_by, page=page, page_size=page_size, query=query)  # type: ignore[arg-type]

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
