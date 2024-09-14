# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from typing import Iterable
from uuid import UUID

from sqlalchemy import and_, select

from data_rentgen.db.models import Output, OutputType
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.uuid import (
    extract_timestamp_from_uuid,
    generate_incremental_uuid,
)
from data_rentgen.dto import OutputDTO


class OutputRepository(Repository[Output]):
    async def create_or_update(
        self,
        output: OutputDTO,
        operation_id: UUID,
        dataset_id: int,
        schema_id: int | None,
    ) -> Output:
        # `created_at' and `id` fields of output should correlate with `operation.id`,
        # to avoid scanning all partitions and speed up insert queries
        created_at = extract_timestamp_from_uuid(operation_id)

        # instead of using UniqueConstraint on multiple fields, one of which (schema_id) can be NULL,
        # use them to calculate unique id
        id_components = f"{operation_id}.{dataset_id}.{output.type}.{schema_id}"
        output_id = generate_incremental_uuid(created_at, id_components.encode("utf-8"))

        result = await self._get(created_at, output_id)
        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock(output_id)
            result = await self._get(created_at, output_id)

        if not result:
            return await self._create(created_at, output_id, output, operation_id, dataset_id, schema_id)
        return await self._update(result, output)

    async def list_by_operation_ids(
        self,
        operation_ids: Iterable[UUID],
        since: datetime,
        until: datetime | None,
    ) -> list[Output]:
        filters = [
            Output.created_at >= since,
            Output.operation_id.in_(operation_ids),
        ]
        if until:
            filters.append(Output.created_at <= until)
        query = select(Output).where(and_(*filters))
        result = await self._session.scalars(query)
        return list(result.all())

    async def list_by_dataset_ids(
        self,
        dataset_ids: Iterable[int],
        since: datetime,
        until: datetime | None,
    ) -> list[Output]:
        filters = [
            Output.created_at >= since,
            Output.dataset_id.in_(dataset_ids),
        ]
        if until:
            filters.append(Output.created_at <= until)
        query = select(Output).where(and_(*filters))
        result = await self._session.scalars(query)
        return list(result.all())

    async def _get(self, created_at: datetime, output_id: UUID) -> Output | None:
        query = select(Output).where(Output.created_at == created_at, Output.id == output_id)
        return await self._session.scalar(query)

    async def _create(
        self,
        created_at: datetime,
        output_id: UUID,
        output: OutputDTO,
        operation_id: UUID,
        dataset_id: int,
        schema_id: int | None = None,
    ) -> Output:
        result = Output(
            created_at=created_at,
            id=output_id,
            operation_id=operation_id,
            dataset_id=dataset_id,
            type=OutputType(output.type),
            schema_id=schema_id,
            num_bytes=output.num_bytes,
            num_rows=output.num_rows,
            num_files=output.num_files,
        )
        self._session.add(result)
        await self._session.flush([result])
        return result

    async def _update(self, existing: Output, new: OutputDTO) -> Output:
        if new.num_bytes is not None:
            existing.num_bytes = new.num_bytes
        if new.num_rows is not None:
            existing.num_rows = new.num_rows
        if new.num_files is not None:
            existing.num_files = new.num_files
        await self._session.flush([existing])
        return existing