# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from typing import Optional

from sqlalchemy import and_, select
from sqlalchemy.orm import selectinload
from uuid6 import UUID

from data_rentgen.db.models import Run, RunStartReason, Status
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.uuid import extract_timestamp_from_uuid
from data_rentgen.dto import PaginationDTO, RunDTO


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

    async def pagination_by_id(self, page: int, page_size: int, run_id: list[int]) -> PaginationDTO[Run]:
        query = select(Run).where(Run.id.in_(run_id)).options(selectinload(Run.started_by_user))
        return await self._paginate_by_query(order_by=[Run.id], page=page, page_size=page_size, query=query)

    async def pagination_by_job_id(
        self,
        page: int,
        page_size: int,
        job_id: int,
        since: datetime,
        until: Optional[datetime],
    ) -> PaginationDTO[Run]:
        filter = [Run.created_at >= since, Run.job_id == job_id]
        if until:
            filter.append(Run.created_at <= until)
        query = select(Run).where(and_(*filter)).options(selectinload(Run.started_by_user))
        return await self._paginate_by_query(order_by=[Run.id], page=page, page_size=page_size, query=query)

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
