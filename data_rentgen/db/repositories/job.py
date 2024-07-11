# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import SQLColumnExpression, select
from sqlalchemy.dialects.postgresql import insert

from data_rentgen.db.models import Job
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import JobDTO, PaginationDTO


class JobRepository(Repository[Job]):

    async def paginate(self, page: int, page_size: int, job_id: list[int]) -> PaginationDTO[Job]:
        where: list[SQLColumnExpression] = [Job.id.in_(job_id)]
        return await self._paginate(order_by=[Job.id], page=page, page_size=page_size, where=where)

    async def get_or_create(self, job: JobDTO, location_id: int) -> Job:
        statement = select(Job).where(Job.location_id == location_id, Job.name == job.name)
        result = await self._session.scalar(statement)
        if not result:
            result = await self._session.scalar(
                insert(Job).on_conflict_do_nothing().returning(Job),
                {"location_id": location_id, "name": job.name},
                execution_options={"populate_existing": True},
            )
        return result  # type: ignore[return-value]
