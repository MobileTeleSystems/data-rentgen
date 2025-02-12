# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends

from data_rentgen.db.models.dataset import Dataset
from data_rentgen.dto.pagination import PaginationDTO
from data_rentgen.services.uow import UnitOfWork


@dataclass
class DatasetServiceResult:
    data: Dataset


class DatasetServicePaginatedResult(PaginationDTO[DatasetServiceResult]):
    pass


class DatasetService:
    def __init__(self, uow: Annotated[UnitOfWork, Depends()]):
        self._uow = uow

    async def paginate(
        self,
        page: int,
        page_size: int,
        dataset_ids: list[int],
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
            items=[DatasetServiceResult(data=dataset) for dataset in pagination.items],
        )
