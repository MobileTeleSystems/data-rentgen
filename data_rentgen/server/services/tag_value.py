# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Collection
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends

from data_rentgen.dto.pagination import PaginationDTO
from data_rentgen.services.uow import UnitOfWork


@dataclass
class TagValueData:
    id: int
    tag_id: int
    value: str


@dataclass
class TagValueServiceResult:
    id: int
    data: TagValueData


class TagValueServicePaginatedResult(PaginationDTO[TagValueServiceResult]):
    pass


class TagValueService:
    def __init__(self, uow: Annotated[UnitOfWork, Depends()]):
        self._uow = uow

    async def paginate(
        self,
        page: int,
        page_size: int,
        tag_id: int | None,
        tag_value_ids: Collection[int],
        search_query: str | None,
    ) -> TagValueServicePaginatedResult:
        pagination = await self._uow.tag_value.paginate(
            page=page,
            page_size=page_size,
            tag_id=tag_id,
            tag_value_ids=tag_value_ids,
            search_query=search_query,
        )

        return TagValueServicePaginatedResult(
            page=pagination.page,
            page_size=pagination.page_size,
            total_count=pagination.total_count,
            items=[
                TagValueServiceResult(
                    id=tag_value.id,
                    data=TagValueData(
                        id=tag_value.id,
                        tag_id=tag_value.tag_id,
                        value=tag_value.value,
                    ),
                )
                for tag_value in pagination.items
            ],
        )
