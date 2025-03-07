# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from collections import defaultdict
from uuid import UUID

from sqlalchemy import any_, select, text
from sqlalchemy.dialects.postgresql import insert

from data_rentgen.db.models import ColumnLineage
from data_rentgen.db.models.dataset_column_relation import (
    DatasetColumnRelation,
    DatasetColumnRelationType,
)
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import ColumnLineageDTO


class DatasetColumnRelationRepository(Repository[ColumnLineage]):
    async def create_bulk_for_column_lineage(self, items: list[ColumnLineageDTO]) -> None:
        if not items:
            return

        # small optimization to avoid creating the same relations over and over.
        # although we're using ON CONFLICT DO NOTHING, PG still has to perform index scans
        # and to get next sequence value for each row
        fingerprints_to_create = await self._get_missing_fingerprints([item.fingerprint for item in items])
        relations_to_create = [item for item in items if item.fingerprint in fingerprints_to_create]

        if relations_to_create:
            await self._create_dataset_column_relations_bulk(relations_to_create)

    async def _get_missing_fingerprints(self, fingerprints: list[UUID]) -> set[UUID]:
        existing = await self._session.execute(
            select(DatasetColumnRelation.fingerprint.distinct()).where(
                DatasetColumnRelation.fingerprint == any_(fingerprints),  # type: ignore[arg-type]
            ),
        )

        return set(fingerprints) - set(existing.scalars().all())

    async def _create_dataset_column_relations_bulk(self, items: list[ColumnLineageDTO]):
        # we don't have to return anything, so there is no need to use o_conflict_update.
        # also rows are immutable, so there is no need to acquire any lock
        insert_statement = insert(DatasetColumnRelation).on_conflict_do_nothing(
            index_elements=[
                DatasetColumnRelation.fingerprint,
                DatasetColumnRelation.source_column,
                text("coalesce(target_column, '')"),
            ],
        )

        # several ColumnLineageDTO may have the same set of column relations, deduplicate them
        to_insert = defaultdict(list)
        for lineage in items:
            for column_relation in lineage.column_relations:
                key = (lineage.fingerprint, column_relation.source_column, column_relation.target_column)
                to_insert[key].append(column_relation)

        await self._session.execute(
            insert_statement,
            [
                {
                    # id is autoincremental
                    "fingerprint": fingerprint,
                    "source_column": relation.source_column,
                    "target_column": relation.target_column,
                    "type": DatasetColumnRelationType(relation.type).value,
                }
                for (fingerprint, *_), relations in to_insert.items()
                for relation in relations
            ],
        )
