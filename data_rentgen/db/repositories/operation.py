# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from typing import Iterable

from sqlalchemy import select

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
        operation_ids: Iterable[UUID],
    ) -> PaginationDTO[Operation]:
        minimal_created_at = extract_timestamp_from_uuid(min(id for id in operation_ids))
        query = (
            select(Operation).where(Operation.created_at >= minimal_created_at).where(Operation.id.in_(operation_ids))
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
        query = select(Operation).where(Operation.created_at >= since, Operation.run_id == run_id)
        if until:
            query = query.where(Operation.created_at <= until)
        return await self._paginate_by_query(order_by=[Operation.id], page=page, page_size=page_size, query=query)

    async def list_by_run_ids(
        self,
        run_ids: Iterable[UUID],
        since: datetime,
        until: datetime | None,
    ) -> list[Operation]:
        query = select(Operation).where(Operation.created_at >= since, Operation.run_id.in_(run_ids))
        if until:
            query = query.where(Operation.created_at <= until)
        result = await self._session.scalars(query)
        return list(result.all())

    async def list_by_ids(self, operation_ids: Iterable[UUID]) -> list[Operation]:
        created_at = extract_timestamp_from_uuid(min(i for i in operation_ids))
        query = select(Operation).where(Operation.created_at >= created_at, Operation.id.in_(operation_ids))
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
