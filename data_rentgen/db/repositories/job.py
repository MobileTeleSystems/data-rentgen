# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from string import punctuation
from typing import Sequence

from sqlalchemy import CompoundSelect, Select, any_, desc, func, select, union
from sqlalchemy.orm import selectinload

from data_rentgen.db.models import Address, Job, JobType, Location
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import JobDTO, PaginationDTO

ts_query_punctuation_map = str.maketrans(punctuation, " " * len(punctuation))


class JobRepository(Repository[Job]):
    async def paginate(
        self,
        page: int,
        page_size: int,
        job_ids: Sequence[int],
        search_query: str | None,
    ) -> PaginationDTO[Job]:
        where = []
        if job_ids:
            where.append(Job.id == any_(job_ids))  # type: ignore[arg-type]

        query: Select | CompoundSelect
        if search_query:
            # For more accurate full-text search, we create a tsquery by combining the `search_query` "as is" with
            # a modified version of it using the '||' operator.
            # The "as is" version is used so that an exact match with the query has the highest rank.
            # The modified version is needed because, in some cases, PostgreSQL tokenizes words joined by punctuation marks
            # (e.g., `database.schema.table`) as a single word. By replacing punctuation with spaces using `translate`,
            # we split such strings into separate words, allowing us to search by parts of the name.
            ts_query = select(
                func.plainto_tsquery("english", search_query).op("||")(
                    func.plainto_tsquery("english", search_query.translate(ts_query_punctuation_map)),
                ),
            ).scalar_subquery()

            job_stmt = (
                select(Job, func.ts_rank(Job.search_vector, ts_query).label("search_rank"))
                .join(Location, Job.location_id == Location.id)
                .join(Address, Location.id == Address.location_id)
                .where(Job.search_vector.op("@@")(ts_query), *where)
            )
            location_stmt = (
                select(Job, func.ts_rank(Location.search_vector, ts_query).label("search_rank"))
                .join(Address, Location.id == Address.location_id)
                .join(Job, Location.id == Job.location_id)
                .where(Location.search_vector.op("@@")(ts_query), *where)
            )
            address_stmt = (
                select(Job, func.ts_rank(Address.search_vector, ts_query).label("search_rank"))
                .join(Location, Address.location_id == Location.id)
                .join(Job, Location.id == Job.location_id)
                .where(Address.search_vector.op("@@")(ts_query), *where)
            )

            query = union(job_stmt, location_stmt, address_stmt)
            order_by = [desc("search_rank"), Job.name]
        else:
            query = select(Job).where(*where)
            order_by = [Job.name]

        options = [selectinload(Job.location).selectinload(Location.addresses)]
        return await self._paginate_by_query(
            query=query,
            order_by=order_by,
            options=options,
            page=page,
            page_size=page_size,
        )

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

    async def list_by_ids(self, job_ids: Sequence[int]) -> list[Job]:
        if not job_ids:
            return []
        query = (
            select(Job)
            .where(Job.id == any_(job_ids))  # type: ignore[arg-type]
            .options(selectinload(Job.location).selectinload(Location.addresses))
        )
        result = await self._session.scalars(query)
        return list(result.all())

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
