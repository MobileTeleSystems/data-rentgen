# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Sequence
from datetime import datetime
from typing import NamedTuple
from uuid import UUID

from sqlalchemy import ColumnElement, any_, func, select
from sqlalchemy.dialects.postgresql import insert

from data_rentgen.db.models import ColumnLineage, DatasetColumnRelation
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.uuid import (
    extract_timestamp_from_uuid,
    generate_incremental_uuid,
)
from data_rentgen.dto import ColumnLineageDTO


class ColumnLineageRow(NamedTuple):
    source_dataset_id: int
    target_dataset_id: int
    source_column: str
    target_column: str
    types_combined: int
    last_used_at: datetime


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

    async def list_by_job_ids(
        self,
        job_ids: Sequence[int],
        since: datetime,
        until: datetime | None,
        source_ids: Sequence[int],
        target_ids: Sequence[int],
    ):
        if not job_ids:
            return []

        where = [
            ColumnLineage.created_at >= since,
            ColumnLineage.job_id == any_(job_ids),  # type: ignore[arg-type]
            ColumnLineage.source_dataset_id == any_(source_ids),  # type: ignore[arg-type]
            ColumnLineage.target_dataset_id == any_(target_ids),  # type: ignore[arg-type]
        ]
        if until:
            where.append(ColumnLineage.created_at <= until)

        return await self._get_column_lineage_with_column_relations(where)

    async def list_by_run_ids(
        self,
        run_ids: Sequence[UUID],
        since: datetime,
        until: datetime | None,
        source_ids: Sequence[int],
        target_ids: Sequence[int],
    ):
        if not run_ids:
            return []

        where = [
            ColumnLineage.created_at >= since,
            ColumnLineage.run_id == any_(run_ids),  # type: ignore[arg-type]
            ColumnLineage.source_dataset_id == any_(source_ids),  # type: ignore[arg-type]
            ColumnLineage.target_dataset_id == any_(target_ids),  # type: ignore[arg-type]
        ]
        if until:
            where.append(ColumnLineage.created_at <= until)
        return await self._get_column_lineage_with_column_relations(where)

    async def list_by_operation_ids(
        self,
        operation_ids: Sequence[UUID],
        source_ids: Sequence[int],
        target_ids: Sequence[int],
    ):
        if not operation_ids:
            return []

        # Output created_at is always the same as operation's created_at
        # do not use `tuple_(Output.created_at, Output.operation_id).in_(...),
        # as this is too complex filter for Postgres to make an optimal query plan
        min_created_at = extract_timestamp_from_uuid(min(operation_ids))
        max_created_at = extract_timestamp_from_uuid(max(operation_ids))
        where = [
            ColumnLineage.created_at >= min_created_at,
            ColumnLineage.created_at <= max_created_at,
            ColumnLineage.operation_id == any_(operation_ids),  # type: ignore[arg-type]
            ColumnLineage.source_dataset_id == any_(source_ids),  # type: ignore[arg-type]
            ColumnLineage.target_dataset_id == any_(target_ids),  # type: ignore[arg-type]
        ]
        return await self._get_column_lineage_with_column_relations(where)

    async def _get_column_lineage_with_column_relations(self, where: list[ColumnElement]):
        query = (
            select(
                ColumnLineage.source_dataset_id,
                ColumnLineage.target_dataset_id,
                DatasetColumnRelation.source_column,
                DatasetColumnRelation.target_column,
                func.bit_or(DatasetColumnRelation.type).label("types_combined"),
                func.max(ColumnLineage.created_at).label("last_used_at"),
            )
            .join(DatasetColumnRelation, ColumnLineage.fingerprint == DatasetColumnRelation.fingerprint)
            .group_by(
                ColumnLineage.source_dataset_id,
                ColumnLineage.target_dataset_id,
                DatasetColumnRelation.source_column,
                DatasetColumnRelation.target_column,
            )
        )

        query = query.where(*where)
        result = await self._session.execute(query)
        return [
            ColumnLineageRow(
                source_dataset_id=row.source_dataset_id,
                target_dataset_id=row.target_dataset_id,
                source_column=row.source_column,
                target_column=row.target_column,
                types_combined=row.types_combined,
                last_used_at=row.last_used_at,
            )
            for row in result.all()
        ]
