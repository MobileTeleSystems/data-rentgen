# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime
from string import punctuation
from typing import Sequence
from uuid import UUID

from sqlalchemy import CompoundSelect, Select, any_, desc, func, select, union
from sqlalchemy.orm import selectinload

from data_rentgen.db.models import Job, Run, RunStartReason, Status
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.uuid import extract_timestamp_from_uuid
from data_rentgen.dto import PaginationDTO, RunDTO

ts_query_punctuation_map = str.maketrans(punctuation, " " * len(punctuation))


class RunRepository(Repository[Run]):
    async def get_or_create_minimal(self, run: RunDTO, job_id: int) -> Run:
        # avoid calculating created_at twice
        created_at = extract_timestamp_from_uuid(run.id)
        result = await self._get(created_at, run.id)
        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock(run.id)
            result = await self._get(created_at, run.id) or await self._create(created_at, run, job_id)
        return result

    async def create_or_update(
        self,
        run: RunDTO,
        job_id: int,
        parent_run_id: UUID | None,
        started_by_user_id: int | None,
    ) -> Run:
        # avoid calculating created_at twice
        created_at = extract_timestamp_from_uuid(run.id)
        result = await self._get(created_at, run.id)
        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock(run.id)
            result = await self._get(created_at, run.id)

        if not result:
            return await self._create(created_at, run, job_id, parent_run_id, started_by_user_id)
        return await self._update(result, run, parent_run_id, started_by_user_id)

    async def paginate(
        self,
        page: int,
        page_size: int,
        since: datetime | None,
        until: datetime | None,
        run_ids: Sequence[UUID],
        job_id: int | None,
        parent_run_id: UUID | None,
        search_query: str | None,
    ) -> PaginationDTO[Run]:
        # do not use `tuple_(Run.created_at, Run.id).in_(...),
        # as this is too complex filter for Postgres to make an optimal query plan
        where = []
        if run_ids:
            min_run_created_at = extract_timestamp_from_uuid(min(run_ids))
            max_run_created_at = extract_timestamp_from_uuid(max(run_ids))
            min_created_at = max(since, min_run_created_at) if since else min_run_created_at
            max_created_at = min(until, max_run_created_at) if until else max_run_created_at
            where = [
                Run.created_at >= min_created_at,
                Run.created_at <= max_created_at,
                Run.id == any_(run_ids),  # type: ignore[arg-type]
            ]
        else:
            if since:
                where.append(Run.created_at >= since)
            if until:
                where.append(Run.created_at <= until)

        if run_ids:
            where.append(Run.id == any_(run_ids))  # type: ignore[arg-type]
        if job_id:
            where.append(Run.job_id == job_id)
        if parent_run_id:
            where.append(Run.parent_run_id == parent_run_id)

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

            run_stmt = (
                select(Run, func.ts_rank(Run.search_vector, ts_query).label("search_rank"))
                .join(Job, Run.job_id == Job.id)
                .where(Run.search_vector.op("@@")(ts_query), *where)
            )
            job_stmt = (
                select(Run, func.ts_rank(Job.search_vector, ts_query).label("search_rank"))
                .join(Run, Job.id == Run.job_id)
                .where(Job.search_vector.op("@@")(ts_query), *where)
            )
            query = union(run_stmt, job_stmt)
            order_by = [desc("search_rank"), Run.id]
        else:
            query = select(Run).where(*where)
            order_by = [Run.id]

        options = [selectinload(Run.started_by_user)]
        return await self._paginate_by_query(
            query=query,
            order_by=order_by,
            options=options,
            page=page,
            page_size=page_size,
        )

    async def list_by_ids(self, run_ids: Sequence[UUID]) -> list[Run]:
        if not run_ids:
            return []
        # do not use `tuple_(Run.created_at, Run.id).in_(...),
        # as this is too complex filter for Postgres to make an optimal query plan
        min_created_at = extract_timestamp_from_uuid(min(run_ids))
        max_created_at = extract_timestamp_from_uuid(max(run_ids))
        query = (
            select(Run)
            .where(
                Run.created_at >= min_created_at,
                Run.created_at <= max_created_at,
                Run.id == any_(run_ids),  # type: ignore[arg-type]
            )
            .options(selectinload(Run.started_by_user))
        )
        result = await self._session.scalars(query)
        return list(result.all())

    async def list_by_job_ids(self, job_ids: Sequence[int], since: datetime, until: datetime | None) -> list[Run]:
        if not job_ids:
            return []
        query = (
            select(Run)
            .where(
                Run.created_at >= since,
                Run.job_id == any_(job_ids),  # type: ignore[arg-type]
            )
            .options(selectinload(Run.started_by_user))
        )
        if until:
            query = query.where(Run.created_at <= until)
        result = await self._session.scalars(query)
        return list(result.all())

    async def _get(self, created_at: datetime, run_id: UUID) -> Run | None:
        query = select(Run).where(Run.id == run_id, Run.created_at == created_at)
        return await self._session.scalar(query)

    async def _create(
        self,
        created_at: datetime,
        run: RunDTO,
        job_id: int,
        parent_run_id: UUID | None = None,
        started_by_user_id: int | None = None,
    ) -> Run:
        result = Run(
            created_at=created_at,
            id=run.id,
            job_id=job_id,
            status=Status(run.status) if run.status else Status.UNKNOWN,
            parent_run_id=parent_run_id,
            started_at=run.started_at,
            started_by_user_id=started_by_user_id,
            start_reason=RunStartReason(run.start_reason) if run.start_reason else None,
            ended_at=run.ended_at,
            external_id=run.external_id,
            attempt=run.attempt,
            persistent_log_url=run.persistent_log_url,
            running_log_url=run.running_log_url,
        )
        self._session.add(result)
        await self._session.flush([result])
        return result

    async def _update(
        self,
        existing: Run,
        new: RunDTO,
        parent_run_id: UUID | None = None,
        started_by_user_id: int | None = None,
    ) -> Run:
        optional_fields = {
            "status": Status(new.status) if new.status else None,
            "parent_run_id": parent_run_id,
            "started_at": new.started_at,
            "started_by_user_id": started_by_user_id,
            "start_reason": RunStartReason(new.start_reason) if new.start_reason else None,
            "ended_at": new.ended_at,
            "external_id": new.external_id,
            "attempt": new.attempt,
            "persistent_log_url": new.persistent_log_url,
            "running_log_url": new.running_log_url,
        }
        for column, value in optional_fields.items():
            if value is not None:
                setattr(existing, column, value)

        await self._session.flush([existing])
        return existing
