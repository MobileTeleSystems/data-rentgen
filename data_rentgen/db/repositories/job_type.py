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
        query = select(JobType).where(JobType.type == job_type_dto.type)
        result = await self._session.scalar(query)
        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock("job_type", job_type_dto.type)
            result = await self._session.scalar(query)

        if not result:
            result = JobType(type=job_type_dto.type)
            self._session.add(result)
            await self._session.flush([result])
        return result
