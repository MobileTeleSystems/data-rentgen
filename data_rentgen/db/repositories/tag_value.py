# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Collection

from sqlalchemy import (
    ARRAY,
    ColumnElement,
    CompoundSelect,
    Integer,
    Select,
    SQLColumnExpression,
    String,
    any_,
    asc,
    bindparam,
    cast,
    desc,
    func,
    select,
    tuple_,
)

from data_rentgen.db.models.tag_value import TagValue
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.search import make_tsquery, ts_match, ts_rank
from data_rentgen.dto.pagination import PaginationDTO
from data_rentgen.dto.tag import TagValueDTO

fetch_bulk_query = select(TagValue).where(
    tuple_(TagValue.tag_id, TagValue.value).in_(
        select(
            func.unnest(
                cast(bindparam("tag_ids"), ARRAY(Integer())),
                cast(bindparam("values"), ARRAY(String())),
            )
            .table_valued("tag_ids", "values")
            .render_derived(),
        ),
    ),
)
get_one_query = select(TagValue).where(TagValue.tag_id == bindparam("tag_id"), TagValue.value == bindparam("value"))


class TagValueRepository(Repository[TagValue]):
    async def paginate(
        self,
        page: int,
        page_size: int,
        tag_id: int | None,
        tag_value_ids: Collection[int],
        search_query: str | None,
    ) -> PaginationDTO[TagValue]:
        query: Select | CompoundSelect
        order_by: list[ColumnElement | SQLColumnExpression]
        if search_query:
            tsquery = make_tsquery(search_query)
            query = (
                select(
                    TagValue.id,
                    TagValue.tag_id,
                    TagValue.value,
                    ts_rank(TagValue.search_vector, tsquery).label("search_rank"),
                )
                .where(ts_match(TagValue.search_vector, tsquery))
                .order_by(desc("search_rank"))
            )
            order_by = [desc("search_rank"), asc("value")]
        else:
            query = select(TagValue)
            order_by = [TagValue.value]

        if tag_id is not None:
            query = query.where(TagValue.tag_id == tag_id)

        if tag_value_ids:
            query = query.where(TagValue.id == any_(list(tag_value_ids)))  # type: ignore[arg-type]

        return await self._paginate_by_query(
            query=query,
            order_by=order_by,
            page=page,
            page_size=page_size,
        )

    async def fetch_bulk(self, tag_values_dto: list[TagValueDTO]) -> list[tuple[TagValueDTO, TagValue | None]]:
        if not tag_values_dto:
            return []

        scalars = await self._session.scalars(
            fetch_bulk_query,
            {
                "tag_ids": [item.tag.id for item in tag_values_dto],
                "values": [item.value for item in tag_values_dto],
            },
        )
        existing = {(tag_value.tag_id, tag_value.value): tag_value for tag_value in scalars.all()}
        return [
            (tag_value_dto, existing.get((tag_value_dto.tag.id, tag_value_dto.value)))  # type: ignore[arg-type]
            for tag_value_dto in tag_values_dto
        ]

    async def create(self, tag_value_dto: TagValueDTO) -> TagValue:
        # if another worker already created the same row, just use it. if not - create with holding the lock.
        await self._lock(tag_value_dto.tag.id, tag_value_dto.value)
        return await self.get_or_create(tag_value_dto)

    async def get_or_create(self, tag_value_dto: TagValueDTO) -> TagValue:
        return (
            await self._get(tag_value_dto.tag.id, tag_value_dto.value)  # type: ignore[arg-type]
            or await self._create(tag_value_dto)
        )

    async def _get(self, tag_id: int, value: str) -> TagValue | None:
        return await self._session.scalar(get_one_query, {"tag_id": tag_id, "value": value})

    async def _create(self, tag_value: TagValueDTO) -> TagValue:
        result = TagValue(tag_id=tag_value.tag.id, value=tag_value.value)
        self._session.add(result)
        await self._session.flush([result])
        return result
