# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import (
    select,
)

from data_rentgen.db.models import JobType
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import JobTypeDTO


class JobTypeRepository(Repository[JobType]):
    async def get_or_create(self, job_type_dto: JobTypeDTO) -> JobType:
        result = await self._get(job_type_dto)
        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock("job_type", job_type_dto.type)
            result = await self._get(job_type_dto) or await self._create(job_type_dto)
        return result

    async def _get(self, job_type_dto: JobTypeDTO) -> JobType | None:
        query = select(JobType).where(JobType.type == job_type_dto.type)
        return await self._session.scalar(query)

    async def _create(self, job_type_dto: JobTypeDTO) -> JobType:
        result = JobType(type=job_type_dto.type)
        self._session.add(result)
        await self._session.flush([result])
        return result
