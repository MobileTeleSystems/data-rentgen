# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime
from typing import Sequence
from uuid import UUID

from sqlalchemy import (
    ColumnElement,
    CompoundSelect,
    Select,
    SQLColumnExpression,
    any_,
    desc,
    func,
    select,
    union,
)
from sqlalchemy.orm import selectinload

from data_rentgen.db.models import Job, Run, RunStartReason, RunStatus
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.search import make_tsquery, ts_match, ts_rank
from data_rentgen.db.utils.uuid import extract_timestamp_from_uuid
from data_rentgen.dto import PaginationDTO, RunDTO


class RunRepository(Repository[Run]):
    async def create_or_update(self, run: RunDTO) -> Run:
        # avoid calculating created_at twice
        created_at = extract_timestamp_from_uuid(run.id)
        result = await self._get(created_at, run.id)
        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock(run.id)
            result = await self._get(created_at, run.id)

        if not result:
            return await self._create(created_at, run)
        return await self._update(result, run)

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
        order_by: list[ColumnElement | SQLColumnExpression]
        if search_query:
            tsquery = make_tsquery(search_query)

            run_stmt = select(Run, ts_rank(Run.search_vector, tsquery).label("search_rank")).where(
                ts_match(Run.search_vector, tsquery),
                *where,
            )
            job_stmt = (
                select(Run, ts_rank(Job.search_vector, tsquery).label("search_rank"))
                .join(Job, Job.id == Run.job_id)
                .where(ts_match(Job.search_vector, tsquery), *where)
            )

            union_cte = union(run_stmt, job_stmt).cte()

            run_columns = [column for column in union_cte.columns if column.name != "search_rank"]

            query = select(
                *run_columns,
                func.max(union_cte.c.search_rank).label("search_rank"),
            ).group_by(*run_columns)
            # place the most recent runs on top
            order_by = [desc("search_rank"), desc("id")]
        else:
            query = select(Run).where(*where)
            order_by = [Run.id.desc()]

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
    ) -> Run:
        result = Run(
            created_at=created_at,
            id=run.id,
            job_id=run.job.id,
            status=RunStatus(run.status),
            parent_run_id=run.parent_run.id if run.parent_run else None,
            started_at=run.started_at,
            started_by_user_id=run.user.id if run.user else None,
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
    ) -> Run:
        # for parent_run most of fields are None, so we can avoid UPDATE statements if row is unchanged
        optional_fields = {
            # in some cases start event may be send from "unknown" job,
            # but sequential RUNNING and STOPPED events are send with proper job name. Rebound run to latest job.
            "job_id": new.job.id or existing.job_id,
            # Merge new information with existing one
            "status": max(RunStatus(new.status), existing.status),
            "parent_run_id": new.parent_run.id if new.parent_run else None,
            "started_at": new.started_at,
            "started_by_user_id": new.user.id if new.user else None,
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
