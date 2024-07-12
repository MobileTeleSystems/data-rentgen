# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import select
from uuid6 import UUID

from data_rentgen.db.models import Run, Status
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import RunDTO


class RunRepository(Repository[Run]):
    async def get_or_create_minimal(self, run: RunDTO, job_id: int) -> Run:
        query = select(Run).where(Run.id == run.id, Run.created_at == run.created_at)
        result = await self._session.scalar(query)
        if not result:
            result = Run(id=run.id, created_at=run.created_at, job_id=job_id)
            self._session.add(result)
            await self._session.flush([result])

        return result

    async def create_or_update(self, run: RunDTO, job_id: int, parent_run_id: UUID | None) -> Run:
        query = select(Run).where(Run.id == run.id, Run.created_at == run.created_at)
        result = await self._session.scalar(query)
        if not result:
            result = Run(
                id=run.id,
                created_at=run.created_at,
                job_id=job_id,
                status=Status(run.status) if run.status else None,
                parent_run_id=parent_run_id,
                started_at=run.started_at,
                ended_at=run.ended_at,
                external_id=run.external_id,
                attempt=run.attempt,
                persistent_log_url=run.persistent_log_url,
                running_log_url=run.running_log_url,
            )
            self._session.add(result)
        else:
            optional_fields = {
                "status": Status(run.status) if run.status else None,
                "parent_run_id": parent_run_id,
                "started_at": run.started_at,
                "ended_at": run.ended_at,
                "external_id": run.external_id,
                "attempt": run.attempt,
                "persistent_log_url": run.persistent_log_url,
                "running_log_url": run.running_log_url,
            }

            for column, value in optional_fields.items():
                if value is not None:
                    setattr(result, column, value)

        await self._session.flush([result])
        return result
