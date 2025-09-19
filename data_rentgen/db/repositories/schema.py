# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Collection

from sqlalchemy import any_, select

from data_rentgen.db.models import Schema
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import SchemaDTO


class SchemaRepository(Repository[Schema]):
    async def list_by_ids(self, schema_ids: Collection[int]) -> list[Schema]:
        if not schema_ids:
            return []

        query = select(Schema).where(Schema.id == any_(list(schema_ids)))  # type: ignore[arg-type]
        result = await self._session.scalars(query)
        return list(result.all())

    async def fetch_known_ids(self, schemas_dto: list[SchemaDTO]) -> list[tuple[SchemaDTO, int | None]]:
        if not schemas_dto:
            return []

        unique_digests = [schema_dto.digest for schema_dto in schemas_dto]
        # schema JSON can be heavy, avoid loading it if not needed
        statement = select(Schema.digest, Schema.id).where(
            Schema.digest == any_(unique_digests),  # type: ignore[arg-type]
        )
        scalars = await self._session.execute(statement)
        known_ids = {item.digest: item.id for item in scalars.all()}
        return [
            (
                schema_dto,
                known_ids.get(schema_dto.digest),
            )
            for schema_dto in schemas_dto
        ]

    async def create(self, schema: SchemaDTO) -> Schema:
        # if another worker already created the same row, just use it. if not - create with holding the lock.
        await self._lock(schema.digest)
        return await self._get(schema) or await self._create(schema)

    async def _get(self, schema: SchemaDTO) -> Schema | None:
        result = select(Schema).where(Schema.digest == schema.digest)
        return await self._session.scalar(result)

    async def _create(self, schema: SchemaDTO) -> Schema:
        result = Schema(digest=schema.digest, fields=schema.fields)
        self._session.add(result)
        await self._session.flush([result])
        return result
