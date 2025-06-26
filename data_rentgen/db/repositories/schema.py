# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Collection

from sqlalchemy import any_, select

from data_rentgen.db.models import Schema
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import SchemaDTO


class SchemaRepository(Repository[Schema]):
    async def get_or_create(self, schema: SchemaDTO) -> Schema:
        result = await self._get(schema)
        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock(schema.digest)
            result = await self._get(schema) or await self._create(schema)
        return result

    async def list_by_ids(self, schema_ids: Collection[int]) -> list[Schema]:
        if not schema_ids:
            return []

        query = select(Schema).where(Schema.id == any_(list(schema_ids)))  # type: ignore[arg-type]
        result = await self._session.scalars(query)
        return list(result.all())

    async def _get(self, schema: SchemaDTO) -> Schema | None:
        result = select(Schema).where(Schema.digest == schema.digest)
        return await self._session.scalar(result)

    async def _create(self, schema: SchemaDTO) -> Schema:
        result = Schema(digest=schema.digest, fields=schema.fields)
        self._session.add(result)
        await self._session.flush([result])
        return result
