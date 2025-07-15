# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Collection
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends

from data_rentgen.db.models.location import Location
from data_rentgen.dto.pagination import PaginationDTO
from data_rentgen.services.uow import UnitOfWork


@dataclass
class TagData:
    name: str
    value: str


@dataclass
class DatasetData:
    id: int
    name: str
    location: Location
    tags: list[TagData]
    schema = None


@dataclass
class DatasetServiceResult:
    id: int
    data: DatasetData


class DatasetServicePaginatedResult(PaginationDTO[DatasetServiceResult]):
    pass


class DatasetService:
    def __init__(self, uow: Annotated[UnitOfWork, Depends()]):
        self._uow = uow

    async def paginate(
        self,
        page: int,
        page_size: int,
        dataset_ids: Collection[int],
        search_query: str | None,
    ) -> DatasetServicePaginatedResult:
        pagination = await self._uow.dataset.paginate(
            page=page,
            page_size=page_size,
            dataset_ids=dataset_ids,
            search_query=search_query,
        )

        return DatasetServicePaginatedResult(
            page=pagination.page,
            page_size=pagination.page_size,
            total_count=pagination.total_count,
            items=[
                DatasetServiceResult(
                    id=dataset.id,
                    data=DatasetData(
                        id=dataset.id,
                        name=dataset.name,
                        location=dataset.location,
                        tags=[TagData(name=tag.tag.name, value=tag.value) for tag in dataset.tags],
                    ),
                )
                for dataset in pagination.items
            ],
        )
