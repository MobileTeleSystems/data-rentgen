# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from uuid import UUID

from sqlalchemy import select

from data_rentgen.db.models import Schema
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.fields import get_fields_digest
from data_rentgen.dto import SchemaDTO


class SchemaRepository(Repository[Schema]):
    async def get_or_create(self, schema: SchemaDTO) -> Schema:
        # avoid calculating digest twice
        digest = get_fields_digest(schema.fields)
        result = await self._get(digest)
        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock(digest)
            result = await self._get(digest) or await self._create(digest, schema)
        return result

    async def _get(self, digest: UUID) -> Schema | None:
        statement = select(Schema).where(Schema.digest == digest)
        return await self._session.scalar(statement)

    async def _create(self, digest: UUID, schema: SchemaDTO) -> Schema:
        result = Schema(digest=digest, fields=schema.fields)
        self._session.add(result)
        await self._session.flush([result])
        return result
