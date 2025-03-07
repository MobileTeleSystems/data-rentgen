# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Literal
from uuid import UUID

from sqlalchemy import ColumnElement, Row, Select, any_, func, literal_column, select
from sqlalchemy.dialects.postgresql import insert

from data_rentgen.db.models import Output, OutputType
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.uuid import (
    extract_timestamp_from_uuid,
    generate_incremental_uuid,
)
from data_rentgen.dto import OutputDTO


class OutputRepository(Repository[Output]):
    def get_id(self, output: OutputDTO) -> UUID:
        # `created_at' field of output should be the same as operation's,
        # to avoid scanning all partitions and speed up queries
        created_at = extract_timestamp_from_uuid(output.operation.id)

        # instead of using UniqueConstraint on multiple fields, one of which (schema_id) can be NULL,
        # use them to calculate unique id
        id_components = [
            str(output.operation.id),
            str(output.dataset.id),
            str(output.schema.id) if output.schema else "",
        ]
        return generate_incremental_uuid(created_at, ".".join(id_components))

    async def create_or_update_bulk(self, outputs: list[OutputDTO]) -> None:
        if not outputs:
            return

        insert_statement = insert(Output)
        new_row = insert_statement.excluded
        statement = insert_statement.on_conflict_do_update(
            index_elements=[Output.created_at, Output.id],
            set_={
                "num_bytes": func.greatest(new_row.num_bytes, Output.num_bytes),
                "num_rows": func.greatest(new_row.num_rows, Output.num_rows),
                "num_files": func.greatest(new_row.num_files, Output.num_files),
            },
        )

        await self._session.execute(
            statement,
            [
                {
                    "id": self.get_id(output),
                    "created_at": extract_timestamp_from_uuid(output.operation.id),
                    "type": OutputType(output.type),
                    "operation_id": output.operation.id,
                    "run_id": output.operation.run.id,
                    "job_id": output.operation.run.job.id,  # type: ignore[arg-type]
                    "dataset_id": output.dataset.id,  # type: ignore[arg-type]
                    "schema_id": output.schema.id if output.schema else None,
                    "num_bytes": output.num_bytes,
                    "num_rows": output.num_rows,
                    "num_files": output.num_files,
                }
                for output in outputs
            ],
        )

    async def list_by_operation_ids(
        self,
        operation_ids: Sequence[UUID],
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[Output]:
        if not operation_ids:
            return []

        # Output created_at is always the same as operation's created_at
        # do not use `tuple_(Output.created_at, Output.operation_id).in_(...),
        # as this is too complex filter for Postgres to make an optimal query plan
        min_created_at = extract_timestamp_from_uuid(min(operation_ids))
        max_created_at = extract_timestamp_from_uuid(max(operation_ids))
        where = [
            Output.created_at >= min_created_at,
            Output.created_at <= max_created_at,
            Output.operation_id == any_(operation_ids),  # type: ignore[arg-type]
        ]

        return await self._get_outputs(where, granularity)

    async def list_by_run_ids(
        self,
        run_ids: Sequence[UUID],
        since: datetime,
        until: datetime | None,
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[Output]:
        if not run_ids:
            return []

        min_run_created_at = extract_timestamp_from_uuid(min(run_ids))
        min_created_at = max(min_run_created_at, since.astimezone(timezone.utc))

        where = [
            Output.created_at >= min_created_at,
            Output.run_id == any_(run_ids),  # type: ignore[arg-type]
        ]
        if until:
            where.append(Output.created_at <= until)

        return await self._get_outputs(where, granularity)

    async def list_by_job_ids(
        self,
        job_ids: Sequence[int],
        since: datetime,
        until: datetime | None,
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[Output]:
        if not job_ids:
            return []

        where = [
            Output.created_at >= since,
            Output.job_id == any_(job_ids),  # type: ignore[arg-type]
        ]
        if until:
            where.append(Output.created_at <= until)

        return await self._get_outputs(where, granularity)

    async def list_by_dataset_ids(
        self,
        dataset_ids: Sequence[int],
        since: datetime,
        until: datetime | None,
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[Output]:
        if not dataset_ids:
            return []

        where = [
            Output.created_at >= since,
            Output.dataset_id == any_(dataset_ids),  # type: ignore[arg-type]
        ]
        if until:
            where.append(Output.created_at <= until)

        return await self._get_outputs(where, granularity)

    async def _get_outputs(
        self,
        where: list[ColumnElement],
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[Output]:
        if granularity == "OPERATION":
            # return Output as-is
            simple_query = select(Output).where(*where)
            result = await self._session.scalars(simple_query)
            return list(result.all())

        # return an aggregated Output
        query: Select[tuple]
        if granularity == "RUN":
            query = select(
                func.max(Output.created_at).label("created_at"),
                literal_column("NULL").label("id"),
                literal_column("NULL").label("operation_id"),
                Output.run_id,
                Output.job_id,
                Output.dataset_id,
                func.sum(Output.num_bytes).label("sum_num_bytes"),
                func.sum(Output.num_rows).label("sum_num_rows"),
                func.sum(Output.num_files).label("sum_num_files"),
                func.min(Output.type).label("min_type"),
                func.max(Output.type).label("max_type"),
                func.min(Output.schema_id).label("min_schema_id"),
                func.max(Output.schema_id).label("max_schema_id"),
            ).group_by(
                Output.run_id,
                Output.job_id,
                Output.dataset_id,
            )
        else:
            query = select(
                func.max(Output.created_at).label("created_at"),
                literal_column("NULL").label("id"),
                literal_column("NULL").label("operation_id"),
                literal_column("NULL").label("run_id"),
                Output.job_id,
                Output.dataset_id,
                func.sum(Output.num_bytes).label("sum_num_bytes"),
                func.sum(Output.num_rows).label("sum_num_rows"),
                func.sum(Output.num_files).label("sum_num_files"),
                func.min(Output.type).label("min_type"),
                func.max(Output.type).label("max_type"),
                func.min(Output.schema_id).label("min_schema_id"),
                func.max(Output.schema_id).label("max_schema_id"),
            ).group_by(
                Output.job_id,
                Output.dataset_id,
            )

        query = query.where(*where)
        results = await self._session.execute(query)
        return [
            Output(
                created_at=row.created_at,
                run_id=row.run_id,
                job_id=row.job_id,
                dataset_id=row.dataset_id,
                num_bytes=row.sum_num_bytes,
                num_rows=row.sum_num_rows,
                num_files=row.sum_num_files,
                # If all outputs within Dataset -> Run|Job have the same type, save it.
                # If not, it's impossible to merge.
                type=row.max_type if row.min_type == row.max_type else None,
                # Same for schema
                schema_id=row.max_schema_id if row.min_schema_id == row.max_schema_id else None,
            )
            for row in results.all()
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
                Output.operation_id.label("operation_id"),
                func.count(Output.dataset_id.distinct()).label("total_datasets"),
                func.sum(Output.num_bytes).label("total_bytes"),
                func.sum(Output.num_rows).label("total_rows"),
                func.sum(Output.num_files).label("total_files"),
            )
            .where(
                Output.created_at >= min_created_at,
                Output.created_at <= max_created_at,
                Output.operation_id == any_(operation_ids),  # type: ignore[arg-type]
            )
            .group_by(Output.operation_id)
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
                Output.run_id.label("run_id"),
                func.count(Output.dataset_id.distinct()).label("total_datasets"),
                func.sum(Output.num_bytes).label("total_bytes"),
                func.sum(Output.num_rows).label("total_rows"),
                func.sum(Output.num_files).label("total_files"),
            )
            .where(
                Output.created_at >= min_created_at,
                Output.run_id == any_(run_ids),  # type: ignore[arg-type]
            )
            .group_by(Output.run_id)
        )

        query_result = await self._session.execute(query)
        return {row.run_id: row for row in query_result.all()}
