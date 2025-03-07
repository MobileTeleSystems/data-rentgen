# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Literal
from uuid import UUID

from sqlalchemy import ColumnElement, Row, Select, any_, func, literal_column, select
from sqlalchemy.dialects.postgresql import insert

from data_rentgen.db.models import Input
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.uuid import (
    extract_timestamp_from_uuid,
    generate_incremental_uuid,
)
from data_rentgen.dto import InputDTO


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
        operation_ids: Sequence[UUID],
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[Input]:
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
            Input.operation_id == any_(operation_ids),  # type: ignore[arg-type]
        ]

        return await self._get_inputs(where, granularity)

    async def list_by_run_ids(
        self,
        run_ids: Sequence[UUID],
        since: datetime,
        until: datetime | None,
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[Input]:
        if not run_ids:
            return []

        min_run_created_at = extract_timestamp_from_uuid(min(run_ids))
        min_created_at = max(min_run_created_at, since.astimezone(timezone.utc))

        where = [
            Input.created_at >= min_created_at,
            Input.run_id == any_(run_ids),  # type: ignore[arg-type]
        ]
        if until:
            where.append(Input.created_at <= until)

        return await self._get_inputs(where, granularity)

    async def list_by_job_ids(
        self,
        job_ids: Sequence[int],
        since: datetime,
        until: datetime | None,
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[Input]:
        if not job_ids:
            return []

        where = [
            Input.created_at >= since,
            Input.job_id == any_(job_ids),  # type: ignore[arg-type]
        ]
        if until:
            where.append(Input.created_at <= until)

        return await self._get_inputs(where, granularity)

    async def list_by_dataset_ids(
        self,
        dataset_ids: Sequence[int],
        since: datetime,
        until: datetime | None,
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[Input]:
        if not dataset_ids:
            return []

        where = [
            Input.created_at >= since,
            Input.dataset_id == any_(dataset_ids),  # type: ignore[arg-type]
        ]
        if until:
            where.append(Input.created_at <= until)

        return await self._get_inputs(where, granularity)

    async def _get_inputs(
        self,
        where: list[ColumnElement],
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[Input]:
        if granularity == "OPERATION":
            # return Input as-is
            simple_query = select(Input).where(*where)
            result = await self._session.scalars(simple_query)
            return list(result.all())

        # return an aggregated Input
        query: Select[tuple]
        if granularity == "RUN":
            query = select(
                func.max(Input.created_at).label("created_at"),
                literal_column("NULL").label("operation_id"),
                Input.run_id,
                Input.job_id,
                Input.dataset_id,
                func.sum(Input.num_bytes).label("sum_num_bytes"),
                func.sum(Input.num_rows).label("sum_num_rows"),
                func.sum(Input.num_files).label("sum_num_files"),
                func.min(Input.schema_id).label("min_schema_id"),
                func.max(Input.schema_id).label("max_schema_id"),
            ).group_by(
                Input.run_id,
                Input.job_id,
                Input.dataset_id,
            )
        else:
            query = select(
                func.max(Input.created_at).label("created_at"),
                literal_column("NULL").label("operation_id"),
                literal_column("NULL").label("run_id"),
                Input.job_id,
                Input.dataset_id,
                func.sum(Input.num_bytes).label("sum_num_bytes"),
                func.sum(Input.num_rows).label("sum_num_rows"),
                func.sum(Input.num_files).label("sum_num_files"),
                func.min(Input.schema_id).label("min_schema_id"),
                func.max(Input.schema_id).label("max_schema_id"),
            ).group_by(
                Input.job_id,
                Input.dataset_id,
            )

        query = query.where(*where)
        query_result = await self._session.execute(query)
        return [
            Input(
                created_at=row.created_at,
                run_id=row.run_id,
                job_id=row.job_id,
                dataset_id=row.dataset_id,
                num_bytes=row.sum_num_bytes,
                num_rows=row.sum_num_rows,
                num_files=row.sum_num_files,
                # If all outputs within Dataset -> Run|Job have the same schema, save it.
                # If not, it's impossible to merge.
                schema_id=row.max_schema_id if row.min_schema_id == row.max_schema_id else None,
            )
            for row in query_result.all()
        ]

    async def get_stats_by_operation_ids(self, operation_ids: Sequence[UUID]) -> dict[UUID, Row]:
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
                Input.operation_id == any_(operation_ids),  # type: ignore[arg-type]
            )
            .group_by(Input.operation_id)
        )

        query_result = await self._session.execute(query)
        return {row.operation_id: row for row in query_result.all()}

    async def get_stats_by_run_ids(self, run_ids: Sequence[UUID]) -> dict[UUID, Row]:
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
                Input.run_id == any_(run_ids),  # type: ignore[arg-type]
            )
            .group_by(Input.run_id)
        )

        query_result = await self._session.execute(query)
        return {row.run_id: row for row in query_result.all()}
