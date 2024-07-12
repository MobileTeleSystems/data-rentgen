# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

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

        id_components = f"{operation_id}.{dataset_id}.{interaction.type}.{schema_id}"
        interaction_id = generate_incremental_uuid(created_at, id_components.encode("utf-8"))

        query = select(Interaction).where(
            Interaction.created_at == created_at,
            Interaction.id == interaction_id,
        )
        result = await self._session.scalar(query)

        if not result:
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
        else:
            if interaction.num_bytes is not None:
                result.num_bytes = interaction.num_bytes
            if interaction.num_rows is not None:
                result.num_rows = interaction.num_rows
            if interaction.num_files is not None:
                result.num_files = interaction.num_files

        await self._session.flush([result])
        return result
