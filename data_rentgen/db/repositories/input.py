# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal
from uuid import UUID

from sqlalchemy import ColumnElement, Row, any_, func, literal_column, select
from sqlalchemy.dialects.postgresql import insert

from data_rentgen.db.models import Input, Schema
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.uuid import (
    extract_timestamp_from_uuid,
    generate_incremental_uuid,
)
from data_rentgen.dto import InputDTO


@dataclass
class InputRow:
    created_at: datetime
    operation_id: UUID
    run_id: UUID
    job_id: int
    dataset_id: int
    num_bytes: int | None
    num_rows: int | None
    num_files: int | None
    schema_id: int | None = None
    schema_relevance_type: Literal["EXACT_MATCH", "LATEST_KNOWN"] | None = None
    schema: Schema | None = None


class InputRepository(Repository[Input]):
    def get_id(self, input_: InputDTO) -> UUID:
        # `created_at' field of input should be the same as operation's,
        # to avoid scanning all partitions and speed up queries
        created_at = extract_timestamp_from_uuid(input_.operation.id)

        # instead of using UniqueConstraint on multiple fields, one of which (schema_id) can be NULL,
        # use them to calculate unique id
        id_components = [
            str(input_.operation.id),
            str(input_.dataset.id),
            str(input_.schema.id) if input_.schema else "",
        ]
        return generate_incremental_uuid(created_at, ".".join(id_components))

    async def create_or_update_bulk(self, inputs: list[InputDTO]) -> None:
        if not inputs:
            return

        insert_statement = insert(Input)
        new_row = insert_statement.excluded
        statement = insert_statement.on_conflict_do_update(
            index_elements=[Input.created_at, Input.id],
            set_={
                "num_bytes": func.greatest(new_row.num_bytes, Input.num_bytes),
                "num_rows": func.greatest(new_row.num_rows, Input.num_rows),
                "num_files": func.greatest(new_row.num_files, Input.num_files),
            },
        )

        await self._session.execute(
            statement,
            [
                {
                    "id": self.get_id(input_),
                    "created_at": extract_timestamp_from_uuid(input_.operation.id),
                    "operation_id": input_.operation.id,
                    "run_id": input_.operation.run.id,
                    "job_id": input_.operation.run.job.id,  # type: ignore[arg-type]
                    "dataset_id": input_.dataset.id,  # type: ignore[arg-type]
                    "schema_id": input_.schema.id if input_.schema else None,
                    "num_bytes": input_.num_bytes,
                    "num_rows": input_.num_rows,
                    "num_files": input_.num_files,
                }
                for input_ in inputs
            ],
        )

    async def list_by_operation_ids(
        self,
        operation_ids: Collection[UUID],
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[InputRow]:
        if not operation_ids:
            return []

        # Input created_at is always the same as operation's created_at
        # do not use `tuple_(Input.created_at, Input.operation_id).in_(...),
        # as this is too complex filter for Postgres to make an optimal query plan
        min_created_at = extract_timestamp_from_uuid(min(operation_ids))
        max_created_at = extract_timestamp_from_uuid(max(operation_ids))
        where = [
            Input.created_at >= min_created_at,
            Input.created_at <= max_created_at,
            Input.operation_id == any_(list(operation_ids)),  # type: ignore[arg-type]
        ]

        return await self._get_inputs(where, granularity)

    async def list_by_run_ids(
        self,
        run_ids: Collection[UUID],
        since: datetime,
        until: datetime | None,
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[InputRow]:
        if not run_ids:
            return []

        min_run_created_at = extract_timestamp_from_uuid(min(run_ids))
        min_created_at = max(min_run_created_at, since.astimezone(timezone.utc))

        where = [
            Input.created_at >= min_created_at,
            Input.run_id == any_(list(run_ids)),  # type: ignore[arg-type]
        ]
        if until:
            where.append(Input.created_at <= until)

        return await self._get_inputs(where, granularity)

    async def list_by_job_ids(
        self,
        job_ids: Collection[int],
        since: datetime,
        until: datetime | None,
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[InputRow]:
        if not job_ids:
            return []

        where = [
            Input.created_at >= since,
            Input.job_id == any_(list(job_ids)),  # type: ignore[arg-type]
        ]
        if until:
            where.append(Input.created_at <= until)

        return await self._get_inputs(where, granularity)

    async def list_by_dataset_ids(
        self,
        dataset_ids: Collection[int],
        since: datetime,
        until: datetime | None,
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[InputRow]:
        if not dataset_ids:
            return []

        where = [
            Input.created_at >= since,
            Input.dataset_id == any_(list(dataset_ids)),  # type: ignore[arg-type]
        ]
        if until:
            where.append(Input.created_at <= until)

        return await self._get_inputs(where, granularity)

    async def _get_inputs(
        self,
        where: Collection[ColumnElement],
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[InputRow]:
        if granularity == "OPERATION":
            # return Input as-is
            simple_query = select(Input).where(*where)
            result = await self._session.scalars(simple_query)
            return [
                InputRow(
                    created_at=row.created_at,
                    operation_id=row.operation_id,
                    run_id=row.run_id,
                    job_id=row.job_id,
                    dataset_id=row.dataset_id,
                    num_bytes=row.num_bytes,
                    num_rows=row.num_rows,
                    num_files=row.num_files,
                    schema_id=row.schema_id,
                    schema_relevance_type="EXACT_MATCH" if row.schema_id else None,
                )
                for row in result.all()
            ]

        # return an aggregated Input
        if granularity == "RUN":
            partition_by = [Input.run_id, Input.job_id, Input.dataset_id]
            base_query = (
                select(
                    Input,
                    func.first_value(Input.schema_id)
                    .over(partition_by=partition_by, order_by=[Input.created_at, Input.schema_id])
                    .label("oldest_schema_id"),
                    func.last_value(Input.schema_id)
                    .over(partition_by=partition_by, order_by=[Input.created_at, Input.schema_id])
                    .label("newest_schema_id"),
                )
                .where(*where)
                .cte()
            )
            query = select(
                func.max(base_query.c.created_at).label("created_at"),
                literal_column("NULL").label("operation_id"),
                base_query.c.run_id,
                base_query.c.job_id,
                base_query.c.dataset_id,
                func.sum(base_query.c.num_bytes).label("sum_num_bytes"),
                func.sum(base_query.c.num_rows).label("sum_num_rows"),
                func.sum(base_query.c.num_files).label("sum_num_files"),
                func.min(base_query.c.oldest_schema_id).label("min_schema_id"),
                func.max(base_query.c.newest_schema_id).label("max_schema_id"),
            ).group_by(
                base_query.c.run_id,
                base_query.c.job_id,
                base_query.c.dataset_id,
            )
        else:
            partition_by = [Input.job_id, Input.dataset_id]
            base_query = (
                select(
                    Input,
                    func.first_value(Input.schema_id)
                    .over(partition_by=partition_by, order_by=[Input.created_at, Input.schema_id])
                    .label("oldest_schema_id"),
                    func.last_value(Input.schema_id)
                    .over(partition_by=partition_by, order_by=[Input.created_at, Input.schema_id])
                    .label("newest_schema_id"),
                )
                .where(*where)
                .cte()
            )
            query = select(
                func.max(base_query.c.created_at).label("created_at"),
                literal_column("NULL").label("operation_id"),
                literal_column("NULL").label("run_id"),
                base_query.c.job_id,
                base_query.c.dataset_id,
                func.sum(base_query.c.num_bytes).label("sum_num_bytes"),
                func.sum(base_query.c.num_rows).label("sum_num_rows"),
                func.sum(base_query.c.num_files).label("sum_num_files"),
                func.min(base_query.c.oldest_schema_id).label("min_schema_id"),
                func.max(base_query.c.newest_schema_id).label("max_schema_id"),
            ).group_by(
                base_query.c.job_id,
                base_query.c.dataset_id,
            )

        query_result = await self._session.execute(query)

        results = []
        for row in query_result.all():
            schema_relevance_type: Literal["EXACT_MATCH", "LATEST_KNOWN"] | None
            if row.max_schema_id:
                schema_relevance_type = "EXACT_MATCH" if row.min_schema_id == row.max_schema_id else "LATEST_KNOWN"
            else:
                schema_relevance_type = None

            results.append(
                InputRow(
                    created_at=row.created_at,
                    operation_id=row.operation_id,
                    run_id=row.run_id,
                    job_id=row.job_id,
                    dataset_id=row.dataset_id,
                    num_bytes=row.sum_num_bytes,
                    num_rows=row.sum_num_rows,
                    num_files=row.sum_num_files,
                    schema_id=row.max_schema_id,
                    schema_relevance_type=schema_relevance_type,
                ),
            )
        return results

    async def get_stats_by_operation_ids(self, operation_ids: Collection[UUID]) -> dict[UUID, Row]:
        if not operation_ids:
            return {}

        # Input created_at is always the same as operation's created_at
        # do not use `tuple_(Input.created_at, Input.operation_id).in_(...),
        # as this is too complex filter for Postgres to make an optimal query plan
        min_created_at = extract_timestamp_from_uuid(min(operation_ids))
        max_created_at = extract_timestamp_from_uuid(max(operation_ids))

        query = (
            select(
                Input.operation_id.label("operation_id"),
                func.count(Input.dataset_id.distinct()).label("total_datasets"),
                func.sum(Input.num_bytes).label("total_bytes"),
                func.sum(Input.num_rows).label("total_rows"),
                func.sum(Input.num_files).label("total_files"),
            )
            .where(
                Input.created_at >= min_created_at,
                Input.created_at <= max_created_at,
                Input.operation_id == any_(list(operation_ids)),  # type: ignore[arg-type]
            )
            .group_by(Input.operation_id)
        )

        query_result = await self._session.execute(query)
        return {row.operation_id: row for row in query_result.all()}

    async def get_stats_by_run_ids(self, run_ids: Collection[UUID]) -> dict[UUID, Row]:
        if not run_ids:
            return {}

        # unlike list_by_run_ids, we need to get all statistics for specific runs, regardless of time range
        min_created_at = extract_timestamp_from_uuid(min(run_ids))
        query = (
            select(
                Input.run_id.label("run_id"),
                func.count(Input.dataset_id.distinct()).label("total_datasets"),
                func.sum(Input.num_bytes).label("total_bytes"),
                func.sum(Input.num_rows).label("total_rows"),
                func.sum(Input.num_files).label("total_files"),
            )
            .where(
                Input.created_at >= min_created_at,
                Input.run_id == any_(list(run_ids)),  # type: ignore[arg-type]
            )
            .group_by(Input.run_id)
        )

        query_result = await self._session.execute(query)
        return {row.run_id: row for row in query_result.all()}
