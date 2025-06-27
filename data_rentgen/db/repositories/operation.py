# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Collection
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import Row, UnaryExpression, any_, bindparam, func, select, update
from sqlalchemy.dialects.postgresql import insert

from data_rentgen.db.models import Operation, OperationStatus, OperationType
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import OperationDTO, PaginationDTO
from data_rentgen.utils.uuid import extract_timestamp_from_uuid


class OperationRepository(Repository[Operation]):
    async def create_or_update_bulk(self, operations: list[OperationDTO]) -> None:
        if not operations:
            return
        data = [
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
                "sql_query_id": operation.sql_query.id if operation.sql_query else None,
                "group": operation.group,
                "position": operation.position,
            }
            for operation in operations
        ]

        # this replaces all null values with defaults
        await self._session.execute(
            insert(Operation).on_conflict_do_nothing(),
            data,
        )

        # if value is still none, keep existing one
        await self._session.execute(
            update(Operation).values(
                {
                    "name": func.coalesce(bindparam("name"), Operation.name),
                    "type": func.coalesce(bindparam("type"), Operation.type),
                    "status": func.greatest(bindparam("status"), Operation.status),
                    "started_at": func.coalesce(bindparam("started_at"), Operation.started_at),
                    "ended_at": func.coalesce(bindparam("ended_at"), Operation.ended_at),
                    "description": func.coalesce(bindparam("description"), Operation.description),
                    "sql_query_id": func.coalesce(bindparam("sql_query_id"), Operation.sql_query_id),
                    "group": func.coalesce(bindparam("group"), Operation.group),
                    "position": func.coalesce(bindparam("position"), Operation.position),
                },
            ),
            data,
        )

    async def paginate(
        self,
        page: int,
        page_size: int,
        operation_ids: Collection[UUID],
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
            where.append(Operation.id == any_(list(operation_ids)))  # type: ignore[arg-type]

        query = select(Operation).where(*where)
        order_by: list[UnaryExpression] = [Operation.created_at.desc(), Operation.id.desc()]
        return await self._paginate_by_query(
            query=query,
            order_by=order_by,
            page=page,
            page_size=page_size,
        )

    async def list_by_run_ids(
        self,
        run_ids: Collection[UUID],
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
            Operation.run_id == any_(list(run_ids)),  # type: ignore[arg-type]
        )
        if until:
            query = query.where(Operation.created_at <= until)
        result = await self._session.scalars(query)
        return list(result.all())

    async def list_by_ids(self, operation_ids: Collection[UUID]) -> list[Operation]:
        if not operation_ids:
            return []

        # Do not use `tuple_(Operation.created_at, Operation.id).in_(...),
        # as this is too complex filter for Postgres to make an optimal query plan
        query = select(Operation).where(
            Operation.created_at >= extract_timestamp_from_uuid(min(operation_ids)),
            Operation.created_at <= extract_timestamp_from_uuid(max(operation_ids)),
            Operation.id == any_(list(operation_ids)),  # type: ignore[arg-type]
        )
        result = await self._session.scalars(query)
        return list(result.all())

    async def get_stats_by_run_ids(self, run_ids: Collection[UUID]) -> dict[UUID, Row]:
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
                Operation.run_id == any_(list(run_ids)),  # type: ignore[arg-type]
            )
            .group_by(Operation.run_id)
        )

        query_result = await self._session.execute(query)
        return {row.run_id: row for row in query_result.all()}
