# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import select

from data_rentgen.db.models import Job, Location
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import JobDTO


class JobRepository(Repository[Job]):
    async def get_or_create(self, job: JobDTO, location: Location) -> Job:
        statement = select(Job).where(Job.location_id == location.id, Job.name == job.name)
        result = await self._session.scalar(statement)
        if not result:
            result = Job(location_id=location.id, name=job.name)
            self._session.add(result)
            await self._session.flush()
        return result
