# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from sqlalchemy import and_, any_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Input, Output, Schema


@dataclass
class IODatasetRelationRow:
    created_at: datetime
    in_dataset_id: int
    out_dataset_id: int
    types_combined: int | None = None
    output_schema_id: int | None = None
    output_schema_relevance_type: Literal["EXACT_MATCH", "LATEST_KNOWN"] | None = None
    output_schema: Schema | None = None
    input_schema_id: int | None = None
    input_schema_relevance_type: Literal["EXACT_MATCH", "LATEST_KNOWN"] | None = None
    input_schema: Schema | None = None


class IODatasetRelationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_relations(
        self,
        dataset_ids: Collection[int],
        since: datetime,
        until: datetime | None,
        direction: Literal["UPSTREAM", "DOWNSTREAM"],
    ) -> list[IODatasetRelationRow]:
        where = [
            Input.created_at >= since,
            Output.created_at >= since,
        ]
        if direction == "UPSTREAM":
            where.append(Output.dataset_id == any_(list(dataset_ids)))  # type: ignore[arg-type]
        else:
            where.append(Input.dataset_id == any_(list(dataset_ids)))  # type: ignore[arg-type]
        if until:
            where.extend([Output.created_at <= until, Input.created_at <= until])

        unique_ids = [Output.dataset_id, Input.dataset_id]
        order_by = [Output.created_at, Input.created_at, Output.schema_id, Input.schema_id]

        # Avoid sorting multiple times by different keys for performance reason,
        # instead reuse the same window expression
        def window(expr, order_by=None):
            return expr.over(partition_by=unique_ids, order_by=order_by)

        # there is no need in GROUP BY, as we already select distinct values
        query = (
            select(
                Input.dataset_id.label("in_dataset_id"),
                Output.dataset_id.label("out_dataset_id"),
                window(func.max(Output.created_at)).label("max_created_at"),
                window(func.first_value(Output.schema_id), order_by).label("oldest_output_schema_id"),
                window(func.last_value(Output.schema_id), order_by).label("newest_output_schema_id"),
                window(func.first_value(Input.schema_id), order_by).label("oldest_input_schema_id"),
                window(func.last_value(Input.schema_id), order_by).label("newest_input_schema_id"),
            )
            .distinct(*unique_ids)
            .join(
                Input,
                and_(
                    Output.operation_id == Input.operation_id,
                    # Avoid returning table1 -> table1 relations.
                    # TODO: cover case with self-reference via symlink
                    Output.dataset_id != Input.dataset_id,
                ),
            )
            .where(*where)
        )

        query_result = await self._session.execute(query)
        results = []
        for row in query_result.all():
            output_schema_relevance_type: Literal["EXACT_MATCH", "LATEST_KNOWN"] | None
            input_schema_relevance_type: Literal["EXACT_MATCH", "LATEST_KNOWN"] | None
            if row.newest_output_schema_id:
                output_schema_relevance_type = (
                    "EXACT_MATCH" if row.oldest_input_schema_id == row.newest_output_schema_id else "LATEST_KNOWN"
                )
            else:
                output_schema_relevance_type = None

            if row.newest_input_schema_id:
                input_schema_relevance_type = (
                    "EXACT_MATCH" if row.oldest_input_schema_id == row.newest_input_schema_id else "LATEST_KNOWN"
                )
            else:
                input_schema_relevance_type = None
            results.append(
                IODatasetRelationRow(
                    created_at=row.max_created_at,
                    in_dataset_id=row.in_dataset_id,
                    out_dataset_id=row.out_dataset_id,
                    output_schema_id=row.newest_output_schema_id,
                    output_schema_relevance_type=output_schema_relevance_type,
                    input_schema_id=row.newest_input_schema_id,
                    input_schema_relevance_type=input_schema_relevance_type,
                ),
            )
        return results
