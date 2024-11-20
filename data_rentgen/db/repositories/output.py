# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone
from typing import Literal, Sequence
from uuid import UUID

from sqlalchemy import ColumnElement, Select, any_, func, literal_column, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import selectinload

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
        return generate_incremental_uuid(created_at, ".".join(id_components).encode("utf-8"))

    async def create_or_update_bulk(self, outputs: list[OutputDTO]) -> list[Output]:
        if not outputs:
            return []

        insert_statement = insert(Output)
        statement = insert_statement.on_conflict_do_update(
            index_elements=[Output.created_at, Output.id],
            set_={
                "num_bytes": func.greatest(insert_statement.excluded.num_bytes, Output.num_bytes),
                "num_rows": func.greatest(insert_statement.excluded.num_rows, Output.num_rows),
                "num_files": func.greatest(insert_statement.excluded.num_files, Output.num_files),
            },
        ).returning(Output)

        result = await self._session.execute(
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
        return list(result.scalars().all())

    async def list_by_operation_ids(
        self,
        operation_ids: Sequence[UUID],
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

        return await self._get_outputs(where, "OPERATION")

    async def list_by_run_ids(
        self,
        run_ids: Sequence[UUID],
        since: datetime,
        until: datetime | None,
        granularity: Literal["RUN", "OPERATION"],
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
            simple_query = select(Output).where(*where).options(selectinload(Output.schema))
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
                Output.type,
                func.sum(Output.num_bytes).label("sum_num_bytes"),
                func.sum(Output.num_rows).label("sum_num_rows"),
                func.sum(Output.num_files).label("sum_num_files"),
            ).group_by(
                Output.run_id,
                Output.job_id,
                Output.dataset_id,
                Output.type,
            )
        else:
            query = select(
                func.max(Output.created_at).label("created_at"),
                literal_column("NULL").label("id"),
                literal_column("NULL").label("operation_id"),
                literal_column("NULL").label("run_id"),
                Output.job_id,
                Output.dataset_id,
                Output.type,
                func.sum(Output.num_bytes).label("sum_num_bytes"),
                func.sum(Output.num_rows).label("sum_num_rows"),
                func.sum(Output.num_files).label("sum_num_files"),
            ).group_by(
                Output.job_id,
                Output.dataset_id,
                Output.type,
            )

        query = query.where(*where)
        results = await self._session.execute(query)
        return [
            Output(
                created_at=row.created_at,
                run_id=row.run_id,
                job_id=row.job_id,
                dataset_id=row.dataset_id,
                type=row.type,
                num_bytes=row.sum_num_bytes,
                num_rows=row.sum_num_rows,
                num_files=row.sum_num_files,
            )
            for row in results.all()
        ]
