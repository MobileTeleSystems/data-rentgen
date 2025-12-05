# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends

from data_rentgen.db.models.job import Job
from data_rentgen.dto.pagination import PaginationDTO
from data_rentgen.services.uow import UnitOfWork


@dataclass
class JobServiceResult:
    id: int
    data: Job


class JobServicePaginatedResult(PaginationDTO[JobServiceResult]):
    pass


class JobService:
    def __init__(self, uow: Annotated[UnitOfWork, Depends()]):
        self._uow = uow

    async def paginate(
        self,
        page: int,
        page_size: int,
        job_ids: Collection[int],
        job_types: Collection[str],
        location_ids: Collection[int],
        location_types: Collection[str],
        search_query: str | None,
    ) -> JobServicePaginatedResult:
        pagination = await self._uow.job.paginate(
            page=page,
            page_size=page_size,
            job_ids=job_ids,
            job_types=job_types,
            location_ids=location_ids,
            location_types=location_types,
            search_query=search_query,
        )

        return JobServicePaginatedResult(
            page=pagination.page,
            page_size=pagination.page_size,
            total_count=pagination.total_count,
            items=[JobServiceResult(id=job.id, data=job) for job in pagination.items],
        )

    async def get_job_types(self) -> Sequence[str]:
        return await self._uow.job_type.get_job_types()
