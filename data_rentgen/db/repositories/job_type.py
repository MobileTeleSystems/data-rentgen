# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import (
    any_,
    select,
)

from data_rentgen.db.models import JobType
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import JobTypeDTO


class JobTypeRepository(Repository[JobType]):
    async def fetch_bulk(self, job_types_dto: list[JobTypeDTO]) -> list[tuple[JobTypeDTO, JobType | None]]:
        unique_keys = [job_type_dto.type for job_type_dto in job_types_dto]
        statement = select(JobType).where(
            JobType.type == any_(unique_keys),  # type: ignore[arg-type]
        )
        scalars = await self._session.scalars(statement)
        existing = {job.type: job for job in scalars.all()}
        return [(job_type_dto, existing.get(job_type_dto.type)) for job_type_dto in job_types_dto]

    async def create(self, job_type_dto: JobTypeDTO) -> JobType:
        # if another worker already created the same row, just use it. if not - create with holding the lock.
        await self._lock("job_type", job_type_dto.type)
        return await self._get(job_type_dto) or await self._create(job_type_dto)

    async def _get(self, job_type_dto: JobTypeDTO) -> JobType | None:
        query = select(JobType).where(JobType.type == job_type_dto.type)
        return await self._session.scalar(query)

    async def _create(self, job_type_dto: JobTypeDTO) -> JobType:
        result = JobType(type=job_type_dto.type)
        self._session.add(result)
        await self._session.flush([result])
        return result
