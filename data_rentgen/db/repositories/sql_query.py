# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0


from sqlalchemy import any_, select

from data_rentgen.db.models.sql_query import SQLQuery
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import SQLQueryDTO


class SQLQueryRepository(Repository[SQLQuery]):
    async def fetch_known_ids(self, sql_queries_dto: list[SQLQueryDTO]) -> list[tuple[SQLQueryDTO, int | None]]:
        if not sql_queries_dto:
            return []

        unique_fingerprints = [sql_query_dto.fingerprint for sql_query_dto in sql_queries_dto]
        # query text can be heavy, avoid loading it if not needed
        statement = select(SQLQuery.fingerprint, SQLQuery.id).where(
            SQLQuery.fingerprint == any_(unique_fingerprints),  # type: ignore[arg-type]
        )
        scalars = await self._session.execute(statement)
        known_ids = {item.fingerprint: item.id for item in scalars.all()}
        return [
            (
                sql_query_dto,
                known_ids.get(sql_query_dto.fingerprint),
            )
            for sql_query_dto in sql_queries_dto
        ]

    async def create(self, sql_query: SQLQueryDTO) -> SQLQuery:
        # if another worker already created the same row, just use it. if not - create with holding the lock.
        await self._lock(sql_query.fingerprint)
        return await self._get(sql_query) or await self._create(sql_query)

    async def _get(self, sql_query: SQLQueryDTO) -> SQLQuery | None:
        result = select(SQLQuery).where(SQLQuery.fingerprint == sql_query.fingerprint)
        return await self._session.scalar(result)

    async def _create(self, sql_query: SQLQueryDTO) -> SQLQuery:
        result = SQLQuery(fingerprint=sql_query.fingerprint, query=sql_query.query)
        self._session.add(result)
        await self._session.flush([result])
        return result
