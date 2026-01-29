# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Collection

from sqlalchemy import (
    ColumnElement,
    CompoundSelect,
    Select,
    SQLColumnExpression,
    any_,
    asc,
    bindparam,
    desc,
    select,
)

from data_rentgen.db.models import Tag
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.search import make_tsquery, ts_match, ts_rank
from data_rentgen.dto.pagination import PaginationDTO
from data_rentgen.dto.tag import TagDTO

fetch_bulk_query = select(Tag).where(Tag.name == any_(bindparam("names")))
get_one_by_name_query = select(Tag).where(Tag.name == bindparam("name"))


class TagRepository(Repository[Tag]):
    async def paginate(
        self,
        page: int,
        page_size: int,
        tag_ids: Collection[int],
        search_query: str | None,
    ) -> PaginationDTO[Tag]:
        query: Select | CompoundSelect
        order_by: list[ColumnElement | SQLColumnExpression]
        if search_query:
            tsquery = make_tsquery(search_query)
            query = select(
                Tag.id,
                Tag.name,
                ts_rank(Tag.search_vector, tsquery).label("search_rank"),
            ).where(ts_match(Tag.search_vector, tsquery))

            order_by = [desc("search_rank"), asc("name")]
        else:
            query = select(Tag)
            order_by = [Tag.name]

        if tag_ids:
            query = query.where(Tag.id == any_(list(tag_ids)))  # type: ignore[arg-type]

        return await self._paginate_by_query(
            query=query,
            order_by=order_by,
            page=page,
            page_size=page_size,
        )

    async def fetch_bulk(self, tags_dto: list[TagDTO]) -> list[tuple[TagDTO, Tag | None]]:
        if not tags_dto:
            return []

        scalars = await self._session.scalars(
            fetch_bulk_query,
            {
                "names": [item.name for item in tags_dto],
            },
        )
        existing = {tag.name: tag for tag in scalars.all()}
        return [(tag_dto, existing.get(tag_dto.name)) for tag_dto in tags_dto]

    async def create(self, tag_dto: TagDTO) -> Tag:
        # if another worker already created the same row, just use it. if not - create with holding the lock.
        await self._lock(tag_dto.name)
        return await self.get_or_create(tag_dto)

    async def get_or_create(self, tag_dto: TagDTO) -> Tag:
        return await self._get(tag_dto.name) or await self._create(tag_dto)

    async def _get(self, name: str) -> Tag | None:
        return await self._session.scalar(get_one_by_name_query, {"name": name})

    async def _create(self, tag: TagDTO) -> Tag:
        result = Tag(name=tag.name)
        self._session.add(result)
        await self._session.flush([result])
        return result
