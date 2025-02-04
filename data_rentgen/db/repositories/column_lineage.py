# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from uuid import UUID

from sqlalchemy.dialects.postgresql import insert

from data_rentgen.db.models import ColumnLineage
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.uuid import (
    extract_timestamp_from_uuid,
    generate_incremental_uuid,
)
from data_rentgen.dto import ColumnLineageDTO


class ColumnLineageRepository(Repository[ColumnLineage]):
    def get_id(self, item: ColumnLineageDTO) -> UUID:
        # `created_at' field of lineage should be the same as operation's,
        # to avoid scanning all partitions and speed up queries
        created_at = extract_timestamp_from_uuid(item.operation.id)

        # instead of using UniqueConstraint on multiple fields, which may produce a lot of table scans,
        # use them to calculate unique id
        id_components = [
            str(item.operation.id),
            str(item.source_dataset.id),
            str(item.target_dataset.id),
            str(item.fingerprint),
        ]
        return generate_incremental_uuid(created_at, ".".join(id_components))

    async def create_bulk(self, items: list[ColumnLineageDTO]):
        if not items:
            return

        insert_statement = insert(ColumnLineage)
        statement = insert_statement.on_conflict_do_nothing(
            index_elements=[ColumnLineage.created_at, ColumnLineage.id],
        )

        await self._session.execute(
            statement,
            [
                {
                    "id": self.get_id(item),
                    "created_at": extract_timestamp_from_uuid(item.operation.id),
                    "operation_id": item.operation.id,
                    "run_id": item.operation.run.id,
                    "job_id": item.operation.run.job.id,  # type: ignore[arg-type]
                    "source_dataset_id": item.source_dataset.id,
                    "target_dataset_id": item.target_dataset.id,
                    "fingerprint": item.fingerprint,
                }
                for item in items
            ],
        )
