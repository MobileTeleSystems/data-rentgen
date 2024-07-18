# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime

from sqlalchemy import select
from uuid6 import UUID

from data_rentgen.db.models import Interaction, InteractionType
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.uuid import (
    extract_timestamp_from_uuid,
    generate_incremental_uuid,
)
from data_rentgen.dto import InteractionDTO


class InteractionRepository(Repository[Interaction]):
    async def create_or_update(
        self,
        interaction: InteractionDTO,
        operation_id: UUID,
        dataset_id: int,
        schema_id: int | None,
    ) -> Interaction:
        # `created_at' and `id` fields of interaction should correlate with `operation.id`,
        # to avoid scanning all partitions and speed up insert queries
        created_at = extract_timestamp_from_uuid(operation_id)

        # instead of using UniqueConstraint on multiple fields, one of which (schema_id) can be NULL,
        # use them to calculate unique id
        id_components = f"{operation_id}.{dataset_id}.{interaction.type}.{schema_id}"
        interaction_id = generate_incremental_uuid(created_at, id_components.encode("utf-8"))

        result = await self._get(created_at, interaction_id)
        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock(interaction_id)
            result = await self._get(created_at, interaction_id)

        if not result:
            return await self._create(created_at, interaction_id, interaction, operation_id, dataset_id, schema_id)
        return await self._update(result, interaction)

    async def _get(self, created_at: datetime, interaction_id: UUID) -> Interaction | None:
        query = select(Interaction).where(Interaction.created_at == created_at, Interaction.id == interaction_id)
        return await self._session.scalar(query)

    async def _create(
        self,
        created_at: datetime,
        interaction_id: UUID,
        interaction: InteractionDTO,
        operation_id: UUID,
        dataset_id: int,
        schema_id: int | None = None,
    ) -> Interaction:
        result = Interaction(
            created_at=created_at,
            id=interaction_id,
            operation_id=operation_id,
            dataset_id=dataset_id,
            type=InteractionType(interaction.type),
            schema_id=schema_id,
            num_bytes=interaction.num_bytes,
            num_rows=interaction.num_rows,
            num_files=interaction.num_files,
        )
        self._session.add(result)
        await self._session.flush([result])
        return result

    async def _update(self, existing: Interaction, new: InteractionDTO) -> Interaction:
        if new.num_bytes is not None:
            existing.num_bytes = new.num_bytes
        if new.num_rows is not None:
            existing.num_rows = new.num_rows
        if new.num_files is not None:
            existing.num_files = new.num_files
        await self._session.flush([existing])
        return existing
