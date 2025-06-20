# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from uuid import UUID

from sqlalchemy import select

from data_rentgen.db.models.sql_query import SQLQuery
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.uuid import generate_static_uuid
from data_rentgen.dto import SQLQueryDTO


class SQLQueryRepository(Repository[SQLQuery]):
    async def get_or_create(self, sql_query: SQLQueryDTO) -> SQLQuery:
        # avoid calculating fingerprint twice
        fingerprint = generate_static_uuid(sql_query.query)
        result = await self._get(fingerprint)
        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock(fingerprint)
            result = await self._get(fingerprint) or await self._create(fingerprint, sql_query)
        return result

    async def _get(self, fingerprint: UUID) -> SQLQuery | None:
        result = select(SQLQuery).where(SQLQuery.fingerprint == fingerprint)
        return await self._session.scalar(result)

    async def _create(self, fingerprint: UUID, sql_query: SQLQueryDTO) -> SQLQuery:
        result = SQLQuery(fingerprint=fingerprint, query=sql_query.query)
        self._session.add(result)
        await self._session.flush([result])
        return result
