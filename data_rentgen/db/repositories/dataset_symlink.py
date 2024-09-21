# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import Sequence

from sqlalchemy import any_, or_, select

from data_rentgen.db.models.dataset_symlink import DatasetSymlink, DatasetSymlinkType
from data_rentgen.db.repositories.base import Repository


class DatasetSymlinkRepository(Repository[DatasetSymlink]):
    async def create_or_update(
        self,
        from_dataset_id: int,
        to_dataset_id: int,
        symlink_type: DatasetSymlinkType,
    ) -> DatasetSymlink:
        result = await self._get(from_dataset_id, to_dataset_id)
        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock(from_dataset_id, to_dataset_id)
            result = await self._get(from_dataset_id, to_dataset_id)

        if not result:
            return await self._create(from_dataset_id, to_dataset_id, symlink_type)
        return await self._update(result, symlink_type)

    async def list_by_dataset_ids(self, dataset_ids: Sequence[int]) -> list[DatasetSymlink]:
        if not dataset_ids:
            return []

        query = select(DatasetSymlink).where(
            or_(
                DatasetSymlink.from_dataset_id == any_(dataset_ids),  # type: ignore[arg-type]
                DatasetSymlink.to_dataset_id == any_(dataset_ids),  # type: ignore[arg-type]
            ),
        )
        scalars = await self._session.scalars(query)
        return list(scalars.all())

    async def _get(self, from_dataset_id: int, to_dataset_id: int) -> DatasetSymlink | None:
        query = select(DatasetSymlink).where(
            DatasetSymlink.from_dataset_id == from_dataset_id,
            DatasetSymlink.to_dataset_id == to_dataset_id,
        )
        return await self._session.scalar(query)

    async def _create(
        self,
        from_dataset_id: int,
        to_dataset_id: int,
        symlink_type: DatasetSymlinkType,
    ) -> DatasetSymlink:
        result = DatasetSymlink(from_dataset_id=from_dataset_id, to_dataset_id=to_dataset_id, type=symlink_type)
        self._session.add(result)
        await self._session.flush([result])
        return result

    async def _update(self, existing: DatasetSymlink, new_type: DatasetSymlinkType) -> DatasetSymlink:
        existing.type = new_type
        await self._session.flush([existing])
        return existing
