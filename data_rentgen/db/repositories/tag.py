# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Collection

from sqlalchemy import ColumnElement, CompoundSelect, Select, SQLColumnExpression, any_, asc, desc, func, select
from sqlalchemy.orm import selectinload

from data_rentgen.db.models.tag import Tag
from data_rentgen.db.models.tag_value import TagValue
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.search import make_tsquery, ts_match, ts_rank
from data_rentgen.dto.pagination import PaginationDTO


class TagRepository(Repository[Tag]):
    async def paginate(
        self,
        page: int,
        page_size: int,
        tag_ids: Collection[int],
        search_query: str | None,
    ) -> PaginationDTO[Tag]:
        where = []
        if tag_ids:
            where.append(Tag.id == any_(list(tag_ids)))  # type: ignore[arg-type]

        query: Select | CompoundSelect
        order_by: list[ColumnElement | SQLColumnExpression]
        if search_query:
            tsquery = make_tsquery(search_query)

            tag_stmt = select(Tag.id, Tag.name, ts_rank(Tag.search_vector, tsquery).label("search_rank")).where(
                ts_match(Tag.search_vector, tsquery),
                *where,
            )
            value_stmt = (
                select(Tag.id, Tag.name, ts_rank(TagValue.search_vector, tsquery).label("search_rank"))
                .join(TagValue, TagValue.tag_id == Tag.id)
                .where(ts_match(TagValue.search_vector, tsquery), *where)
            )
            union_cte = tag_stmt.union_all(value_stmt).cte("tag_union")
            query = select(
                union_cte.c.id,
                union_cte.c.name,
                func.max(union_cte.c.search_rank).label("search_rank"),
            ).group_by(union_cte.c.id, union_cte.c.name)

            order_by = [desc("search_rank"), asc("name")]
        else:
            query = select(Tag).where(*where)
            order_by = [Tag.name]

        options = [
            selectinload(Tag.tag_values),
        ]

        return await self._paginate_by_query(
            query=query,
            order_by=order_by,
            options=options,
            page=page,
            page_size=page_size,
        )
