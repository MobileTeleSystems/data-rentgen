# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import select

from data_rentgen.db.models import Schema
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.fields import get_fields_digest
from data_rentgen.dto import SchemaDTO


class SchemaRepository(Repository[Schema]):
    async def get_or_create(self, schema: SchemaDTO) -> Schema:
        statement = select(Schema).where(
            Schema.digest == get_fields_digest(schema.fields),
        )
        result = await self._session.scalar(statement)
        if not result:
            result = Schema(
                digest=get_fields_digest(schema.fields),
                fields=schema.fields,
            )
            self._session.add(result)
            await self._session.flush([result])

        return result
