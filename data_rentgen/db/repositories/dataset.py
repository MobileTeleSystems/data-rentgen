# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from string import punctuation
from typing import Sequence

from sqlalchemy import desc, func, select, union
from sqlalchemy.orm import selectinload

from data_rentgen.db.models import Address, Dataset, Location
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import DatasetDTO, PaginationDTO

ts_query_punctuation_map = str.maketrans(punctuation, " " * len(punctuation))


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
        # For more accurate full-text search, we create a tsquery by combining the `search_query` "as is" with
        # a modified version of it using the '||' operator.
        # In the modified version of `search_query`, punctuation is replaced with spaces using `translate`.
        ts_query = select(
            func.plainto_tsquery("english", search_query).op("||")(
                func.plainto_tsquery("english", search_query.translate(ts_query_punctuation_map)),
            ),
        ).scalar_subquery()
        base_stmt = select(Dataset.id)
        dataset_stmt = (
            base_stmt.add_columns((func.ts_rank(Dataset.search_vector, ts_query)).label("search_rank"))
            .join(Location, Dataset.location_id == Location.id)
            .join(Address, Location.id == Address.location_id)
            .where(Dataset.search_vector.op("@@")(ts_query))
        )
        location_stmt = (
            base_stmt.add_columns((func.ts_rank(Location.search_vector, ts_query)).label("search_rank"))
            .join(Address, Location.id == Address.location_id)
            .join(Dataset, Location.id == Dataset.location_id)
            .where(Location.search_vector.op("@@")(ts_query))
        )
        address_stmt = (
            base_stmt.add_columns((func.ts_rank(Address.search_vector, ts_query)).label("search_rank"))
            .join(Location, Address.location_id == Location.id)
            .join(Dataset, Location.id == Dataset.location_id)
            .where(Address.search_vector.op("@@")(ts_query))
        )
        union_query = union(dataset_stmt, location_stmt, address_stmt).order_by(desc("search_rank"))

        results = await self._session.execute(
            union_query.limit(page_size).offset((page - 1) * page_size),
        )
        results = results.all()  # type: ignore[assignment]
        dataset_ids = [result.id for result in results]
        return await self.paginate(page=page, page_size=page_size, dataset_ids=dataset_ids)

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
