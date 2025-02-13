# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
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
        job_ids: list[int],
        search_query: str | None,
    ) -> JobServicePaginatedResult:
        pagination = await self._uow.job.paginate(
            page=page,
            page_size=page_size,
            job_ids=job_ids,
            search_query=search_query,
        )

        return JobServicePaginatedResult(
            page=pagination.page,
            page_size=pagination.page_size,
            total_count=pagination.total_count,
            items=[JobServiceResult(id=job.id, data=job) for job in pagination.items],
        )
