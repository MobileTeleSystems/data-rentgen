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

    async def pagination_by_id(
        self,
        page: int,
        page_size: int,
        operation_ids: Sequence[UUID],
    ) -> PaginationDTO[Operation]:
        # do not use `tuple_(Operation.created_at, Operation.id).in_(...),
        # as this is too complex filter for Postgres to make an optimal query plan
        min_created_at = extract_timestamp_from_uuid(min(operation_ids))
        max_created_at = extract_timestamp_from_uuid(max(operation_ids))
        query = select(Operation).where(
            Operation.created_at >= min_created_at,
            Operation.created_at <= max_created_at,
            Operation.id == any_(operation_ids),  # type: ignore[arg-type]
        )
        return await self._paginate_by_query(
            order_by=[Operation.run_id, Operation.id],
            page=page,
            page_size=page_size,
            query=query,
        )

    async def pagination_by_run_id(
        self,
        page: int,
        page_size: int,
        run_id: UUID,
        since: datetime,
        until: datetime | None,
    ) -> PaginationDTO[Operation]:
        # All operations are created after run
        run_created_at = extract_timestamp_from_uuid(run_id)
        # But user may request a more narrow time window
        min_operation_created_at = max(run_created_at, since.astimezone(timezone.utc))
        query = select(Operation).where(Operation.created_at >= min_operation_created_at, Operation.run_id == run_id)
        if until:
            query = query.where(Operation.created_at <= until)
        return await self._paginate_by_query(order_by=[Operation.id], page=page, page_size=page_size, query=query)

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
            "position": new.position,
        }
        for column, value in optional_fields.items():
            if value is not None:
                setattr(existing, column, value)

        await self._session.flush([existing])
        return existing
