# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Collection

from sqlalchemy import (
    ColumnElement,
    CompoundSelect,
    Row,
    Select,
    SQLColumnExpression,
    any_,
    asc,
    desc,
    func,
    select,
    union,
)
from sqlalchemy.orm import selectinload

from data_rentgen.db.models import Address, Job, Location
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.search import make_tsquery, ts_match, ts_rank
from data_rentgen.dto import JobDTO, PaginationDTO

UNKNOWN_JOB_TYPE = 0


class JobRepository(Repository[Job]):
    async def paginate(
        self,
        page: int,
        page_size: int,
        job_ids: Collection[int],
        search_query: str | None,
    ) -> PaginationDTO[Job]:
        where = []
        if job_ids:
            where.append(Job.id == any_(list(job_ids)))  # type: ignore[arg-type]

        query: Select | CompoundSelect
        order_by: list[ColumnElement | SQLColumnExpression]
        if search_query:
            tsquery = make_tsquery(search_query)

            job_stmt = select(Job, ts_rank(Job.search_vector, tsquery).label("search_rank")).where(
                ts_match(Job.search_vector, tsquery),
                *where,
            )
            location_stmt = (
                select(Job, ts_rank(Location.search_vector, tsquery).label("search_rank"))
                .join(Job, Location.id == Job.location_id)
                .where(ts_match(Location.search_vector, tsquery), *where)
            )
            address_stmt = (
                select(Job, func.max(ts_rank(Address.search_vector, tsquery).label("search_rank")))
                .join(Location, Address.location_id == Location.id)
                .join(Job, Location.id == Job.location_id)
                .where(ts_match(Address.search_vector, tsquery), *where)
                .group_by(Job.id, Location.id, Address.id)
            )

            union_cte = union(job_stmt, location_stmt, address_stmt).cte()

            job_columns = [column for column in union_cte.columns if column.name != "search_rank"]

            query = select(
                *job_columns,
                func.max(union_cte.c.search_rank).label("search_rank"),
            ).group_by(*job_columns)
            order_by = [desc("search_rank"), asc("name")]
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

    async def create_or_update(self, job: JobDTO) -> Job:
        result = await self._get(job)
        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock(job.location.id, job.name)
            result = await self._get(job)

        if not result:
            return await self._create(job)
        return await self._update(result, job)

    async def list_by_ids(self, job_ids: Collection[int]) -> list[Job]:
        if not job_ids:
            return []
        query = (
            select(Job)
            .where(Job.id == any_(list(job_ids)))  # type: ignore[arg-type]
            .options(selectinload(Job.location).selectinload(Location.addresses))
        )
        result = await self._session.scalars(query)
        return list(result.all())

    async def get_stats_by_location_ids(self, location_ids: Collection[int]) -> dict[int, Row]:
        if not location_ids:
            return {}

        query = (
            select(
                Job.location_id.label("location_id"),
                func.count(Job.id.distinct()).label("total_jobs"),
            )
            .where(
                Job.location_id == any_(list(location_ids)),  # type: ignore[arg-type]
            )
            .group_by(Job.location_id)
        )

        query_result = await self._session.execute(query)
        return {row.location_id: row for row in query_result.all()}

    async def _get(self, job: JobDTO) -> Job | None:
        statement = select(Job).where(Job.location_id == job.location.id, Job.name == job.name)
        return await self._session.scalar(statement)

    async def _create(self, job: JobDTO) -> Job:
        result = Job(
            location_id=job.location.id,
            name=job.name,
            type_id=job.type.id if job.type else UNKNOWN_JOB_TYPE,
        )
        self._session.add(result)
        await self._session.flush([result])
        return result

    async def _update(self, existing: Job, new: JobDTO) -> Job:
        # almost of fields are immutable, so we can avoid UPDATE statements if row is unchanged
        if new.type and new.type.id:
            existing.type_id = new.type.id
            await self._session.flush([existing])
        return existing
