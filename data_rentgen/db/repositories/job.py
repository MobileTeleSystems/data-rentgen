# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from string import punctuation
from typing import Iterable

from sqlalchemy import desc, func, select, union
from sqlalchemy.orm import selectinload

from data_rentgen.db.models import Address, Job, JobType, Location
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import JobDTO, PaginationDTO

ts_query_punctuation_map = str.maketrans(punctuation, " " * len(punctuation))


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

    async def list_by_ids(self, job_ids: Iterable[int]) -> list[Job]:
        query = (
            select(Job).where(Job.id.in_(job_ids)).options(selectinload(Job.location).selectinload(Location.addresses))
        )
        result = await self._session.scalars(query)
        return list(result.all())

    async def search(self, search_query: str, page: int, page_size: int) -> PaginationDTO[Job]:
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
        base_stmt = select(Job.id)
        job_stmt = (
            base_stmt.add_columns((func.ts_rank(Job.search_vector, ts_query)).label("search_rank"))
            .join(Location, Job.location_id == Location.id)
            .join(Address, Location.id == Address.location_id)
            .where(Job.search_vector.op("@@")(ts_query))
        )
        location_stmt = (
            base_stmt.add_columns((func.ts_rank(Location.search_vector, ts_query)).label("search_rank"))
            .join(Address, Location.id == Address.location_id)
            .join(Job, Location.id == Job.location_id)
            .where(Location.search_vector.op("@@")(ts_query))
        )
        address_stmt = (
            base_stmt.add_columns((func.ts_rank(Address.search_vector, ts_query)).label("search_rank"))
            .join(Location, Address.location_id == Location.id)
            .join(Job, Location.id == Job.location_id)
            .where(Address.search_vector.op("@@")(ts_query))
        )
        union_query = union(job_stmt, location_stmt, address_stmt).order_by(desc("search_rank"))

        results = await self._session.execute(
            union_query.limit(page_size).offset((page - 1) * page_size),
        )
        results = results.all()  # type: ignore[assignment]
        job_ids = [result.id for result in results]
        return await self.paginate(page=page, page_size=page_size, job_ids=job_ids)

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
