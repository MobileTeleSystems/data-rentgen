# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Collection

from sqlalchemy import ARRAY, BindParameter, Integer, any_, bindparam, cast, func, or_, select, tuple_

from data_rentgen.db.models.dataset_symlink import DatasetSymlink, DatasetSymlinkType
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import DatasetSymlinkDTO


class DatasetSymlinkRepository(Repository[DatasetSymlink]):
    async def fetch_bulk(
        self,
        dataset_symlinks_dto: list[DatasetSymlinkDTO],
    ) -> list[tuple[DatasetSymlinkDTO, DatasetSymlink | None]]:
        if not dataset_symlinks_dto:
            return []

        from_dataset_ids = [dataset_symlink_dto.from_dataset.id for dataset_symlink_dto in dataset_symlinks_dto]
        to_dataset_ids = [dataset_symlink_dto.to_dataset.id for dataset_symlink_dto in dataset_symlinks_dto]

        pairs = (
            func.unnest(
                cast(from_dataset_ids, ARRAY(Integer())),
                cast(to_dataset_ids, ARRAY(Integer())),
            )
            .table_valued("from_dataset_ids", "to_dataset_ids")
            .render_derived()
        )

        statement = select(DatasetSymlink).where(
            tuple_(DatasetSymlink.from_dataset_id, DatasetSymlink.to_dataset_id).in_(select(pairs)),
        )
        scalars = await self._session.scalars(statement)
        existing = {(item.from_dataset_id, item.to_dataset_id): item for item in scalars.all()}
        return [
            (
                dto,
                existing.get((dto.from_dataset.id, dto.to_dataset.id)),  # type: ignore[arg-type]
            )
            for dto in dataset_symlinks_dto
        ]

    async def create(self, dataset_symlink: DatasetSymlinkDTO) -> DatasetSymlink:
        # if another worker already created the same row, just use it. if not - create with holding the lock.
        await self._lock(dataset_symlink.from_dataset.id, dataset_symlink.to_dataset.id)
        return await self._get(dataset_symlink) or await self._create(dataset_symlink)

    async def list_by_dataset_ids(self, dataset_ids: Collection[int]) -> list[DatasetSymlink]:
        if not dataset_ids:
            return []

        param: BindParameter[list[int]] = bindparam("dataset_ids")
        query = select(DatasetSymlink).where(
            or_(
                DatasetSymlink.from_dataset_id == any_(param),
                DatasetSymlink.to_dataset_id == any_(param),
            ),
        )
        scalars = await self._session.scalars(query, {"dataset_ids": list(dataset_ids)})
        return list(scalars.all())

    async def _get(self, dataset_symlink: DatasetSymlinkDTO) -> DatasetSymlink | None:
        query = select(DatasetSymlink).where(
            DatasetSymlink.from_dataset_id == dataset_symlink.from_dataset.id,
            DatasetSymlink.to_dataset_id == dataset_symlink.to_dataset.id,
        )
        return await self._session.scalar(query)

    async def _create(self, dataset_symlink: DatasetSymlinkDTO) -> DatasetSymlink:
        result = DatasetSymlink(
            from_dataset_id=dataset_symlink.from_dataset.id,
            to_dataset_id=dataset_symlink.to_dataset.id,
            type=DatasetSymlinkType(dataset_symlink.type),
        )
        self._session.add(result)
        await self._session.flush([result])
        return result
