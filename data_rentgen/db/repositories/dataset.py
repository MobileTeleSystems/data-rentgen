# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Collection

from sqlalchemy import (
    ColumnElement,
    CompoundSelect,
    Row,
    Select,
    SQLColumnExpression,
    any_,
    asc,
    desc,
    func,
    select,
    union,
)
from sqlalchemy.orm import selectinload

from data_rentgen.db.models import Address, Dataset, Location, TagValue
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.search import make_tsquery, ts_match, ts_rank
from data_rentgen.dto import DatasetDTO, PaginationDTO


class DatasetRepository(Repository[Dataset]):
    async def get_or_create(self, dataset: DatasetDTO) -> Dataset:
        result = await self._get(dataset)

        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock(dataset.location.id, dataset.name)
            result = await self._get(dataset)

        if not result:
            return await self._create(dataset)
        return result

    async def paginate(
        self,
        page: int,
        page_size: int,
        dataset_ids: Collection[int],
        search_query: str | None,
    ) -> PaginationDTO[Dataset]:
        where = []
        if dataset_ids:
            where.append(Dataset.id == any_(list(dataset_ids)))  # type: ignore[arg-type]

        query: Select | CompoundSelect
        order_by: list[ColumnElement | SQLColumnExpression]
        if search_query:
            tsquery = make_tsquery(search_query)

            dataset_stmt = select(Dataset, ts_rank(Dataset.search_vector, tsquery).label("search_rank")).where(
                ts_match(Dataset.search_vector, tsquery),
                *where,
            )
            location_stmt = (
                select(Dataset, ts_rank(Location.search_vector, tsquery).label("search_rank"))
                .join(Dataset, Location.id == Dataset.location_id)
                .where(ts_match(Location.search_vector, tsquery), *where)
            )
            address_stmt = (
                select(Dataset, func.max(ts_rank(Address.search_vector, tsquery)).label("search_rank"))
                .join(Location, Address.location_id == Location.id)
                .join(Dataset, Location.id == Dataset.location_id)
                .where(ts_match(Address.search_vector, tsquery), *where)
                .group_by(Dataset.id, Location.id, Address.id)
            )

            union_cte = union(dataset_stmt, location_stmt, address_stmt).cte()

            dataset_columns = [column for column in union_cte.columns if column.name != "search_rank"]

            query = select(
                *dataset_columns,
                func.max(union_cte.c.search_rank).label("search_rank"),
            ).group_by(*dataset_columns)
            order_by = [desc("search_rank"), asc("name")]
        else:
            query = select(Dataset).where(*where)
            order_by = [Dataset.name]

        options = [
            selectinload(Dataset.location).selectinload(Location.addresses),
            selectinload(Dataset.tags).selectinload(TagValue.tag),
        ]
        return await self._paginate_by_query(
            query=query,
            order_by=order_by,
            options=options,
            page=page,
            page_size=page_size,
        )

    async def list_by_ids(self, dataset_ids: Collection[int]) -> list[Dataset]:
        if not dataset_ids:
            return []
        query = (
            select(Dataset)
            .where(Dataset.id == any_(list(dataset_ids)))  # type: ignore[arg-type]
            .options(selectinload(Dataset.location).selectinload(Location.addresses))
            .options(selectinload(Dataset.tags).selectinload(TagValue.tag))
        )
        result = await self._session.scalars(query)
        return list(result.all())

    async def get_stats_by_location_ids(self, location_ids: Collection[int]) -> dict[int, Row]:
        if not location_ids:
            return {}

        query = (
            select(
                Dataset.location_id.label("location_id"),
                func.count(Dataset.id.distinct()).label("total_datasets"),
            )
            .where(
                Dataset.location_id == any_(list(location_ids)),  # type: ignore[arg-type]
            )
            .group_by(Dataset.location_id)
        )

        query_result = await self._session.execute(query)
        return {row.location_id: row for row in query_result.all()}

    async def _get(self, dataset: DatasetDTO) -> Dataset | None:
        statement = select(Dataset).where(Dataset.location_id == dataset.location.id, Dataset.name == dataset.name)
        return await self._session.scalar(statement)

    async def _create(self, dataset: DatasetDTO) -> Dataset:
        result = Dataset(location_id=dataset.location.id, name=dataset.name)
        self._session.add(result)
        await self._session.flush([result])
        return result
