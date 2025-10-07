# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Sequence

from sqlalchemy import (
    any_,
    bindparam,
    select,
)

from data_rentgen.db.models import JobType
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import JobTypeDTO

fetch_bulk_query = select(JobType).where(
    JobType.type == any_(bindparam("types")),
)

get_one_query = select(JobType).where(
    JobType.type == bindparam("type"),
)

get_distinct_query = select(JobType.type).distinct(JobType.type).order_by(JobType.type)


class JobTypeRepository(Repository[JobType]):
    async def fetch_bulk(self, job_types_dto: list[JobTypeDTO]) -> list[tuple[JobTypeDTO, JobType | None]]:
        if not job_types_dto:
            return []

        scalars = await self._session.scalars(
            fetch_bulk_query,
            {
                "types": [job_type_dto.type for job_type_dto in job_types_dto],
            },
        )
        existing = {job.type: job for job in scalars.all()}
        return [(job_type_dto, existing.get(job_type_dto.type)) for job_type_dto in job_types_dto]

    async def create(self, job_type_dto: JobTypeDTO) -> JobType:
        # if another worker already created the same row, just use it. if not - create with holding the lock.
        await self._lock("job_type", job_type_dto.type)
        return await self._get(job_type_dto) or await self._create(job_type_dto)

    async def get_job_types(self) -> Sequence[str]:
        result = await self._session.scalars(get_distinct_query)
        return result.all()

    async def _get(self, job_type_dto: JobTypeDTO) -> JobType | None:
        return await self._session.scalar(get_one_query, {"type": job_type_dto.type})

    async def _create(self, job_type_dto: JobTypeDTO) -> JobType:
        result = JobType(type=job_type_dto.type)
        self._session.add(result)
        await self._session.flush([result])
        return result
