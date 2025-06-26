# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import select

from data_rentgen.db.models.sql_query import SQLQuery
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import SQLQueryDTO


class SQLQueryRepository(Repository[SQLQuery]):
    async def get_or_create(self, sql_query: SQLQueryDTO) -> SQLQuery:
        result = await self._get(sql_query)
        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock(sql_query.fingerprint)
            result = await self._get(sql_query) or await self._create(sql_query)
        return result

    async def _get(self, sql_query: SQLQueryDTO) -> SQLQuery | None:
        result = select(SQLQuery).where(SQLQuery.fingerprint == sql_query.fingerprint)
        return await self._session.scalar(result)

    async def _create(self, sql_query: SQLQueryDTO) -> SQLQuery:
        result = SQLQuery(fingerprint=sql_query.fingerprint, query=sql_query.query)
        self._session.add(result)
        await self._session.flush([result])
        return result
