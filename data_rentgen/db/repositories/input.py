# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone
from typing import Literal, Sequence
from uuid import UUID

from sqlalchemy import Select, any_, func, literal_column, select
from sqlalchemy.dialects.postgresql import insert

from data_rentgen.db.models import Input
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.uuid import (
    extract_timestamp_from_uuid,
    generate_incremental_uuid,
)
from data_rentgen.dto import InputDTO


class InputRepository(Repository[Input]):
    def get_id(self, input: InputDTO) -> UUID:
        # `created_at' field of input should be the same as operation's,
        # to avoid scanning all partitions and speed up queries
        created_at = extract_timestamp_from_uuid(input.operation.id)

        # instead of using UniqueConstraint on multiple fields, one of which (schema_id) can be NULL,
        # use them to calculate unique id
        id_components = [
            str(input.operation.id),
            str(input.dataset.id),
            str(input.schema.id) if input.schema else "",
        ]
        return generate_incremental_uuid(created_at, ".".join(id_components).encode("utf-8"))

    async def create_or_update_bulk(self, inputs: list[InputDTO]) -> list[Input]:
        if not inputs:
            return []

        insert_statement = insert(Input)
        statement = insert_statement.on_conflict_do_update(
            index_elements=[Input.created_at, Input.id],
            set_={
                "num_bytes": func.coalesce(insert_statement.excluded.num_bytes, Input.num_bytes),
                "num_rows": func.coalesce(insert_statement.excluded.num_rows, Input.num_rows),
                "num_files": func.coalesce(insert_statement.excluded.num_files, Input.num_files),
            },
        ).returning(Input)

        result = await self._session.execute(
            statement,
            [
                {
                    "id": self.get_id(input),
                    "created_at": extract_timestamp_from_uuid(input.operation.id),
                    "operation_id": input.operation.id,
                    "run_id": input.operation.run.id,
                    "job_id": input.operation.run.job.id,  # type: ignore[arg-type]
                    "dataset_id": input.dataset.id,  # type: ignore[arg-type]
                    "schema_id": input.schema.id if input.schema else None,
                    "num_bytes": input.num_bytes,
                    "num_rows": input.num_rows,
                    "num_files": input.num_files,
                }
                for input in inputs
            ],
        )
        return list(result.scalars().all())

    async def list_by_operation_ids(
        self,
        operation_ids: Sequence[UUID],
    ) -> list[Input]:
        if not operation_ids:
            return []

        # Input created_at is always the same as operation's created_at.
        # do not use `tuple_(Input.created_at, Input.operation_id).in_(...),
        # as this is too complex filter for Postgres to make an optimal query plan
        min_created_at = extract_timestamp_from_uuid(min(operation_ids))
        max_created_at = extract_timestamp_from_uuid(max(operation_ids))
        query = select(Input).where(
            Input.created_at >= min_created_at,
            Input.created_at <= max_created_at,
            Input.operation_id == any_(operation_ids),  # type: ignore[arg-type]
        )
        result = await self._session.scalars(query)
        return list(result.all())

    async def list_by_run_ids(
        self,
        run_ids: Sequence[UUID],
        since: datetime,
        until: datetime | None,
        granularity: Literal["RUN", "OPERATION"],
    ) -> list[Input]:
        if not run_ids:
            return []

        min_run_created_at = extract_timestamp_from_uuid(min(run_ids))
        min_created_at = max(min_run_created_at, since.astimezone(timezone.utc))

        query = self._get_select(granularity).where(
            Input.created_at >= min_created_at,
            Input.run_id == any_(run_ids),  # type: ignore[arg-type]
        )
        if until:
            query = query.where(Input.created_at <= until)

        results = await self._session.execute(query)
        # convert tuple of fields to Input object
        return [Input(**row._asdict()) for row in results.all()]  # noqa: WPS437

    async def list_by_job_ids(
        self,
        job_ids: Sequence[int],
        since: datetime,
        until: datetime | None,
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[Input]:
        if not job_ids:
            return []

        query = self._get_select(granularity).where(
            Input.created_at >= since,
            Input.job_id == any_(job_ids),  # type: ignore[arg-type]
        )
        if until:
            query = query.where(Input.created_at <= until)

        results = await self._session.execute(query)
        # convert tuple of fields to Input object
        return [Input(**row._asdict()) for row in results.all()]  # noqa: WPS437

    async def list_by_dataset_ids(
        self,
        dataset_ids: Sequence[int],
        since: datetime,
        until: datetime | None,
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[Input]:
        if not dataset_ids:
            return []

        query = self._get_select(granularity).where(
            Input.created_at >= since,
            Input.dataset_id == any_(dataset_ids),  # type: ignore[arg-type]
        )
        if until:
            query = query.where(Input.created_at <= until)

        results = await self._session.execute(query)
        # convert tuple of fields to Input object
        return [Input(**row._asdict()) for row in results.all()]  # noqa: WPS437

    def _get_select(
        self,
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> Select:
        if granularity == "OPERATION":
            return select(
                Input.created_at,
                Input.id,
                Input.operation_id,
                Input.run_id,
                Input.job_id,
                Input.dataset_id,
                Input.num_bytes,
                Input.num_rows,
                Input.num_files,
            )

        if granularity == "RUN":
            return select(
                func.max(Input.created_at).label("created_at"),
                literal_column("NULL").label("id"),
                literal_column("NULL").label("operation_id"),
                Input.run_id,
                Input.job_id,
                Input.dataset_id,
                func.sum(Input.num_bytes).label("num_bytes"),
                func.sum(Input.num_rows).label("num_rows"),
                func.sum(Input.num_files).label("num_files"),
            ).group_by(
                Input.run_id,
                Input.job_id,
                Input.dataset_id,
            )

        return select(
            func.max(Input.created_at).label("created_at"),
            literal_column("NULL").label("id"),
            literal_column("NULL").label("operation_id"),
            literal_column("NULL").label("run_id"),
            Input.job_id,
            Input.dataset_id,
            func.sum(Input.num_bytes).label("num_bytes"),
            func.sum(Input.num_rows).label("num_rows"),
            func.sum(Input.num_files).label("num_files"),
        ).group_by(
            Input.job_id,
            Input.dataset_id,
        )
