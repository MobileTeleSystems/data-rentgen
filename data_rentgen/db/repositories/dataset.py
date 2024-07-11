# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from data_rentgen.db.models import Dataset
from data_rentgen.db.models.dataset_symlink import DatasetSymlink, DatasetSymlinkType
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import DatasetDTO


class DatasetRepository(Repository[Dataset]):
    async def create_or_update(self, dataset: DatasetDTO, location_id: int) -> Dataset:
        statement = select(Dataset).where(
            Dataset.location_id == location_id,
            Dataset.name == dataset.name,
        )
        result = await self._session.scalar(statement)
        if not result:
            insert_statement = insert(Dataset)
            insert_statement = insert_statement.on_conflict_do_update(
                index_elements=[Dataset.location_id, Dataset.name],
                set_={"format": dataset.format},
            )
            result = await self._session.scalar(
                insert_statement.returning(Dataset),
                {
                    "location_id": location_id,
                    "name": dataset.name,
                    "format": dataset.format,
                },
                execution_options={"populate_existing": True},
            )

        return result  # type: ignore[return-value]

    async def create_or_update_symlink(
        self,
        from_dataset_id: int,
        to_dataset_id: int,
        symlink_type: DatasetSymlinkType,
    ) -> DatasetSymlink:
        statement = select(DatasetSymlink).where(
            DatasetSymlink.from_dataset_id == from_dataset_id,
            DatasetSymlink.to_dataset_id == to_dataset_id,
        )
        result = await self._session.scalar(statement)
        if not result:
            insert_statement = insert(DatasetSymlink)
            insert_statement = insert_statement.on_conflict_do_update(
                index_elements=[DatasetSymlink.from_dataset_id, DatasetSymlink.to_dataset_id],
                set_={"type": symlink_type},
            )
            result = await self._session.scalar(
                insert_statement.returning(DatasetSymlink),
                {
                    "from_dataset_id": from_dataset_id,
                    "to_dataset_id": to_dataset_id,
                    "type": symlink_type,
                },
                execution_options={"populate_existing": True},
            )

        return result  # type: ignore[return-value]
