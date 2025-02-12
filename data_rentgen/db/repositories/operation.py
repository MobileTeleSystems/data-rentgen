# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone
from typing import Sequence

from sqlalchemy import Row, any_, func, select
from sqlalchemy.dialects.postgresql import insert

from data_rentgen.db.models import Operation, OperationStatus, OperationType
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.uuid import extract_timestamp_from_uuid
from data_rentgen.dto import OperationDTO, PaginationDTO
from data_rentgen.utils import UUID


class OperationRepository(Repository[Operation]):
    async def create_or_update_bulk(self, operations: list[OperationDTO]) -> list[Operation]:
        if not operations:
            return []

        insert_statement = insert(Operation)
        statement = insert_statement.on_conflict_do_update(
            index_elements=[Operation.created_at, Operation.id],
            set_={
                "name": func.coalesce(insert_statement.excluded.name, Operation.name),
                "type": func.coalesce(insert_statement.excluded.type, Operation.type),
                "status": func.greatest(insert_statement.excluded.status, Operation.status),
                "started_at": func.coalesce(insert_statement.excluded.started_at, Operation.started_at),
                "ended_at": func.coalesce(insert_statement.excluded.ended_at, Operation.ended_at),
                "description": func.coalesce(insert_statement.excluded.description, Operation.description),
                "group": func.coalesce(insert_statement.excluded.group, Operation.group),
                "position": func.coalesce(insert_statement.excluded.position, Operation.position),
            },
        ).returning(Operation)

        result = await self._session.execute(
            statement,
            [
                {
                    "id": operation.id,
                    "created_at": extract_timestamp_from_uuid(operation.id),
                    "run_id": operation.run.id,
                    "name": operation.name,
                    "type": OperationType(operation.type) if operation.type else None,
                    "status": OperationStatus(operation.status),
                    "started_at": operation.started_at,
                    "ended_at": operation.ended_at,
                    "description": operation.description,
                    "group": operation.group,
                    "position": operation.position,
                }
                for operation in operations
            ],
        )
        return list(result.scalars().all())

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

    async def get_stats_by_run_ids(self, run_ids: Sequence[UUID]) -> dict[UUID, Row]:
        if not run_ids:
            return {}

        # unlike list_by_run_ids, we need to get all statistics for specific runs, regardless of time range
        min_created_at = extract_timestamp_from_uuid(min(run_ids))
        query = (
            select(
                Operation.run_id.label("run_id"),
                func.count(Operation.id.distinct()).label("total_operations"),
            )
            .where(
                Operation.created_at >= min_created_at,
                Operation.run_id == any_(run_ids),  # type: ignore[arg-type]
            )
            .group_by(Operation.run_id)
        )

        query_result = await self._session.execute(query)
        return {row.run_id: row for row in query_result.all()}
