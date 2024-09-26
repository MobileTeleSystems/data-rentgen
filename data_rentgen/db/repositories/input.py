# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from typing import Sequence
from uuid import UUID

from sqlalchemy import ColumnElement, and_, any_, func, select

from data_rentgen.db.models import Input
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.uuid import (
    extract_timestamp_from_uuid,
    generate_incremental_uuid,
)
from data_rentgen.dto import InputDTO


class InputRepository(Repository[Input]):
    async def create_or_update(
        self,
        input: InputDTO,
        operation_id: UUID,
        run_id: UUID,
        job_id: int,
        dataset_id: int,
        schema_id: int | None,
    ) -> Input:
        # `created_at' field of input should be the same as operation's,
        # to avoid scanning all partitions and speed up queries
        created_at = extract_timestamp_from_uuid(operation_id)

        # instead of using UniqueConstraint on multiple fields, one of which (schema_id) can be NULL,
        # use them to calculate unique id
        id_components = f"{operation_id}.{dataset_id}.{schema_id}"
        input_id = generate_incremental_uuid(created_at, id_components.encode("utf-8"))

        result = await self._get(created_at, input_id)
        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock(input_id)
            result = await self._get(created_at, input_id)

        if not result:
            return await self._create(
                created_at=created_at,
                input_id=input_id,
                input=input,
                operation_id=operation_id,
                run_id=run_id,
                job_id=job_id,
                dataset_id=dataset_id,
                schema_id=schema_id,
            )
        return await self._update(result, input)

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

    async def list_by_operation_ids_grouped_by_run(
        self,
        operation_ids: Sequence[UUID],
    ) -> list[Input]:
        # Input created_at is always the same as operation's created_at
        # do not use `tuple_(Input.created_at, Input.operation_id).in_(...),
        # as this is too complex filter for Postgres to make an optimal query plan
        if not operation_ids:
            return []

        min_created_at = extract_timestamp_from_uuid(min(operation_ids))
        max_created_at = extract_timestamp_from_uuid(max(operation_ids))
        filters = [
            Input.created_at >= min_created_at,
            Input.created_at <= max_created_at,
            Input.run_id == any_(operation_ids),  # type: ignore[arg-type]
        ]

        results = await self._list_with_aggregation(filters=filters, aggregation_field="run_id")
        return [
            Input(
                created_at=None,
                id=None,
                operation_id=None,
                job_id=None,
                run_id=row[0],
                dataset_id=row[1],
                num_bytes=row[2],
                num_rows=row[3],
                num_files=row[4],
            )
            for row in results
        ]

    async def list_by_operation_ids_goruped_by_job(
        self,
        operation_ids: Sequence[UUID],
    ) -> list[Input]:
        # Input created_at is always the same as operation's created_at
        # do not use `tuple_(Input.created_at, Input.operation_id).in_(...),
        # as this is too complex filter for Postgres to make an optimal query plan
        if not operation_ids:
            return []

        min_created_at = extract_timestamp_from_uuid(min(operation_ids))
        max_created_at = extract_timestamp_from_uuid(max(operation_ids))
        filters = [
            Input.created_at >= min_created_at,
            Input.created_at <= max_created_at,
            Input.run_id == any_(operation_ids),  # type: ignore[arg-type]
        ]

        results = await self._list_with_aggregation(filters=filters, aggregation_field="run_id")
        return [
            Input(
                created_at=None,
                id=None,
                operation_id=None,
                job_id=row[0],
                run_id=None,
                dataset_id=row[1],
                num_bytes=row[2],
                num_rows=row[3],
                num_files=row[4],
            )
            for row in results
        ]

    async def list_by_dataset_ids(
        self,
        dataset_ids: Sequence[int],
        since: datetime,
        until: datetime | None,
    ) -> list[Input]:
        if not dataset_ids:
            return []

        filters = [
            Input.created_at >= since,
            Input.dataset_id == any_(dataset_ids),  # type: ignore[arg-type]
        ]
        if until:
            filters.append(Input.created_at <= until)
        query = select(Input).where(and_(*filters))
        result = await self._session.scalars(query)
        return list(result.all())

    async def list_by_dataset_ids_grouped_by_run(
        self,
        dataset_ids: Sequence[int],
        since: datetime,
        until: datetime | None,
    ) -> list[Input]:
        if not dataset_ids:
            return []
        filters = [
            Input.created_at >= since,
            Input.dataset_id == any_(dataset_ids),  # type: ignore[arg-type]
        ]
        if until:
            filters.append(Input.created_at <= until)
        results = await self._list_with_aggregation(filters=filters, aggregation_field="run_id")
        return [
            Input(
                created_at=None,
                id=None,
                operation_id=None,
                job_id=None,
                run_id=row[0],
                dataset_id=row[1],
                num_bytes=row[2],
                num_rows=row[3],
                num_files=row[4],
            )
            for row in results
        ]

    async def list_by_dataset_ids_grouped_by_job(
        self,
        dataset_ids: Sequence[int],
        since: datetime,
        until: datetime | None,
    ):
        if not dataset_ids:
            return []
        filters = [
            Input.created_at >= since,
            Input.dataset_id == any_(dataset_ids),  # type: ignore[arg-type]
        ]
        if until:
            filters.append(Input.created_at <= until)
        results = await self._list_with_aggregation(filters=filters, aggregation_field="job_id")
        return [
            Input(
                created_at=None,
                id=None,
                operation_id=None,
                job_id=row[0],
                run_id=None,
                dataset_id=row[1],
                num_bytes=row[2],
                num_rows=row[3],
                num_files=row[4],
            )
            for row in results
        ]

    async def _get(self, created_at: datetime, input_id: UUID) -> Input | None:
        query = select(Input).where(Input.created_at == created_at, Input.id == input_id)
        return await self._session.scalar(query)

    async def _create(
        self,
        created_at: datetime,
        input_id: UUID,
        input: InputDTO,
        operation_id: UUID,
        run_id: UUID,
        job_id: int,
        dataset_id: int,
        schema_id: int | None = None,
    ) -> Input:
        result = Input(
            created_at=created_at,
            id=input_id,
            operation_id=operation_id,
            run_id=run_id,
            job_id=job_id,
            dataset_id=dataset_id,
            schema_id=schema_id,
            num_bytes=input.num_bytes,
            num_rows=input.num_rows,
            num_files=input.num_files,
        )
        self._session.add(result)
        await self._session.flush([result])
        return result

    async def _update(self, existing: Input, new: InputDTO) -> Input:
        if new.num_bytes is not None:
            existing.num_bytes = new.num_bytes
        if new.num_rows is not None:
            existing.num_rows = new.num_rows
        if new.num_files is not None:
            existing.num_files = new.num_files
        await self._session.flush([existing])
        return existing

    async def _list_with_aggregation(self, filters: list[ColumnElement[bool]], aggregation_field: str):
        query = (
            select(
                getattr(Input, aggregation_field),
                Input.dataset_id,
                func.sum(Input.num_bytes).label("num_bytes"),
                func.sum(Input.num_rows).label("num_rows"),
                func.sum(Input.num_files).label("num_files"),
            )
            .where(and_(*filters))
            .group_by(
                getattr(Input, aggregation_field),
                Input.dataset_id,
            )
        )
        result = await self._session.execute(query)

        return result.all()
