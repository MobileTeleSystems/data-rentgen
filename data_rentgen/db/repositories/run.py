# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from uuid6 import UUID

from data_rentgen.db.models import Run, Status
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import RunDTO


class RunRepository(Repository[Run]):
    async def get_or_create_minimal(self, run: RunDTO, job_id: int) -> Run:
        statement = select(Run).where(
            Run.id == run.id,
            Run.created_at == run.created_at,
        )
        result = await self._session.scalar(statement)
        if not result:
            result = await self._session.scalar(
                insert(Run).on_conflict_do_nothing().returning(Run),
                {"job_id": job_id, "id": run.id, "created_at": run.created_at},
                execution_options={"populate_existing": True},
            )

        return result  # type: ignore[return-value]

    async def create_or_update(self, run: RunDTO, job_id: int, parent_run_id: UUID | None) -> Run:
        mandatory_fields = {
            "created_at": run.created_at,
            "id": run.id,
            "job_id": job_id,
        }
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

        insert_statement = insert(Run)
        insert_statement = insert_statement.on_conflict_do_update(
            index_elements=[Run.created_at, Run.id],
            set_={
                # Different runEvents may have different parts of information - one only 'started_at', second only 'ended_at',
                # third only 'parent_run_id'. If field value is unknown, we should keep original value
                getattr(Run, column): getattr(insert_statement.excluded, column)
                for column, value in optional_fields.items()
                if value is not None
            },
        )
        return await self._session.scalar(  # type: ignore[return-value]
            insert_statement.returning(Run),
            {
                **mandatory_fields,
                **optional_fields,
            },
            execution_options={"populate_existing": True},
        )
