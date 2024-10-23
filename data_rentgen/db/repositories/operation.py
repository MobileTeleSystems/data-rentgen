# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone
from typing import Sequence

from sqlalchemy import any_, select

from data_rentgen.db.models import Operation, OperationType, Status
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.uuid import extract_timestamp_from_uuid
from data_rentgen.dto import OperationDTO, PaginationDTO
from data_rentgen.utils import UUID


class OperationRepository(Repository[Operation]):
    async def create_or_update(self, operation: OperationDTO, run_id: UUID | None) -> Operation:
        # avoid calculating created_at twice
        created_at = extract_timestamp_from_uuid(operation.id)
        result = await self._get(created_at, operation.id)
        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock(operation.id)
            result = await self._get(created_at, operation.id)

        if not result:
            # run_id is always present in first event, but may be missing in later ones
            return await self._create(created_at, operation, run_id)  # type: ignore[arg-type]
        return await self._update(result, operation)

    async def paginate(
        self,
        page: int,
        page_size: int,
        operation_ids: Sequence[UUID],
        since: datetime | None,
        until: datetime | None,
        run_id: UUID | None,
    ) -> PaginationDTO[Operation]:
        # do not use `tuple_(Operation.created_at, Operation.id).in_(...),
        # as this is too complex filter for Postgres to make an optimal query plan
        where = []
        if operation_ids:
            min_operation_created_at = extract_timestamp_from_uuid(min(operation_ids))
            max_operation_created_at = extract_timestamp_from_uuid(max(operation_ids))
            min_created_at = max(since, min_operation_created_at) if since else min_operation_created_at
            max_created_at = min(until, max_operation_created_at) if until else max_operation_created_at
            where = [
                Operation.created_at >= min_created_at,
                Operation.created_at <= max_created_at,
            ]
        else:
            if since:
                where.append(Operation.created_at >= since)
            if until:
                where.append(Operation.created_at <= until)

        if run_id:
            where.append(Operation.run_id == run_id)
        if operation_ids:
            where.append(Operation.id == any_(operation_ids))  # type: ignore[arg-type]

        query = select(Operation).where(*where)
        order_by = [Operation.run_id, Operation.id.desc()]
        return await self._paginate_by_query(
            query=query,
            order_by=order_by,
            page=page,
            page_size=page_size,
        )

    async def list_by_run_ids(
        self,
        run_ids: Sequence[UUID],
        since: datetime,
        until: datetime | None,
    ) -> list[Operation]:
        if not run_ids:
            return []

        # All operations are created after run
        min_run_created_at = extract_timestamp_from_uuid(min(run_ids))
        min_operation_created_at = max(min_run_created_at, since.astimezone(timezone.utc))
        query = select(Operation).where(
            Operation.created_at >= min_operation_created_at,
            Operation.run_id == any_(run_ids),  # type: ignore[arg-type]
        )
        if until:
            query = query.where(Operation.created_at <= until)
        result = await self._session.scalars(query)
        return list(result.all())

    async def list_by_ids(self, operation_ids: Sequence[UUID]) -> list[Operation]:
        if not operation_ids:
            return []
        # do not use `tuple_(Operation.created_at, Operation.id).in_(...),
        # as this is too complex filter for Postgres to make an optimal query plan
        min_created_at = extract_timestamp_from_uuid(min(operation_ids))
        max_created_at = extract_timestamp_from_uuid(max(operation_ids))
        query = select(Operation).where(
            Operation.created_at >= min_created_at,
            Operation.created_at <= max_created_at,
            Operation.id == any_(operation_ids),  # type: ignore[arg-type]
        )
        result = await self._session.scalars(query)
        return list(result.all())

    async def _get(self, created_at: datetime, operation_id: UUID) -> Operation | None:
        query = select(Operation).where(Operation.created_at == created_at, Operation.id == operation_id)
        return await self._session.scalar(query)

    async def _create(self, created_at: datetime, operation: OperationDTO, run_id: UUID) -> Operation:
        result = Operation(
            created_at=created_at,
            id=operation.id,
            run_id=run_id,
            name=operation.name,
            type=OperationType(operation.type),
            status=Status(operation.status) if operation.status else Status.UNKNOWN,
            started_at=operation.started_at,
            ended_at=operation.ended_at,
            description=operation.description,
            group=operation.group,
            position=operation.position,
        )
        self._session.add(result)
        await self._session.flush([result])
        return result

    async def _update(self, existing: Operation, new: OperationDTO) -> Operation:
        optional_fields = {
            "type": OperationType(new.type) if new.type else None,
            "status": Status(new.status) if new.status else None,
            "started_at": new.started_at,
            "ended_at": new.ended_at,
            "description": new.description,
            "group": new.group,
            "position": new.position,
        }
        for column, value in optional_fields.items():
            if value is not None:
                setattr(existing, column, value)

        await self._session.flush([existing])
        return existing
