# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
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
        search_query: str | None,
        location_id: int | None,
        job_type: Collection[str],
    ) -> JobServicePaginatedResult:
        pagination = await self._uow.job.paginate(
            page=page,
            page_size=page_size,
            job_ids=job_ids,
            search_query=search_query,
            location_id=location_id,
            job_type=job_type,
        )

        return JobServicePaginatedResult(
            page=pagination.page,
            page_size=pagination.page_size,
            total_count=pagination.total_count,
            items=[JobServiceResult(id=job.id, data=job) for job in pagination.items],
        )

    async def get_job_types(self) -> Sequence[str]:
        return await self._uow.job_type.get_job_types()
