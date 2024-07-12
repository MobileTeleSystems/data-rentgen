# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import select

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
            result = Dataset(location_id=location_id, name=dataset.name, format=dataset.format)
            self._session.add(result)
            await self._session.flush([result])

        return result

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
            result = DatasetSymlink(from_dataset_id=from_dataset_id, to_dataset_id=to_dataset_id, type=symlink_type)
            self._session.add(result)
            await self._session.flush([result])

        elif result.type:
            result.type = symlink_type
            await self._session.flush([result])

        return result
