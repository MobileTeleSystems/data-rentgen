# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime

from sqlalchemy import and_, select
from sqlalchemy.orm import selectinload

from data_rentgen.db.models import Job, JobType, Location, Run
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import JobDTO, PaginationDTO


class JobRepository(Repository[Job]):
    async def paginate(self, page: int, page_size: int, job_ids: list[int]) -> PaginationDTO[Job]:
        query = select(Job).options(selectinload(Job.location).selectinload(Location.addresses))
        if job_ids:
            query = query.where(Job.id.in_(job_ids))
        return await self._paginate_by_query(order_by=[Job.name], page=page, page_size=page_size, query=query)

    async def create_or_update(self, job: JobDTO, location_id: int) -> Job:
        result = await self._get(location_id, job.name)
        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock(location_id, job.name)
            result = await self._get(location_id, job.name)

        if not result:
            return await self._create(job, location_id)
        return await self._update(result, job)

    async def get_job_runs(self, job_id: int, since: datetime, until: datetime | None):
        filter = [Run.created_at >= since, Job.id == job_id]
        if until:
            filter.append(Run.created_at <= until)
        query = select(Job.id, Run.id).join(Run, Job.id == Run.job_id).where(and_(*filter))
        result = await self._session.execute(query)
        return result.all()

    async def get_node_info(self, job_id: int):
        query = select(Job.id, Job.name, Job.type).where(Job.id == job_id)
        result = await self._session.execute(query)
        return result.one()

    async def _get(self, location_id: int, name: str) -> Job | None:
        statement = select(Job).where(Job.location_id == location_id, Job.name == name)
        return await self._session.scalar(statement)

    async def _create(self, job: JobDTO, location_id: int) -> Job:
        result = Job(
            location_id=location_id,
            name=job.name,
            type=JobType(job.type) if job.type else JobType.UNKNOWN,
        )
        self._session.add(result)
        await self._session.flush([result])
        return result

    async def _update(self, existing: Job, new: JobDTO) -> Job:
        if new.type:
            existing.type = JobType(new.type)
            await self._session.flush([existing])
        return existing
