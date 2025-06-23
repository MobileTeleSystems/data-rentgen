# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from sqlalchemy import any_, func, select
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
        output_partition_by = [Output.run_id, Output.dataset_id]
        output_order_by = [Output.created_at, Output.schema_id]
        input_partition_by = [Input.run_id, Input.dataset_id]
        input_order_by = [Input.created_at, Input.schema_id]
        match direction:
            case "UPSTREAM":
                # dasasets as Outputs
                where = [
                    Output.created_at >= since,
                    Input.created_at >= since,
                    Output.dataset_id == any_(list(dataset_ids)),  # type: ignore[arg-type]
                ]
                if until:
                    where.extend([Output.created_at <= until, Input.created_at <= until])

                base_query = (
                    select(
                        Input.dataset_id.label("in_dataset_id"),
                        Output.created_at.label("created_at"),
                        Output.dataset_id.label("out_dataset_id"),
                        func.first_value(Output.schema_id)
                        .over(partition_by=output_partition_by, order_by=output_order_by)
                        .label("oldest_output_schema_id"),
                        func.last_value(Output.schema_id)
                        .over(partition_by=output_partition_by, order_by=output_order_by)
                        .label("newest_output_schema_id"),
                        func.first_value(Input.schema_id)
                        .over(partition_by=input_partition_by, order_by=input_order_by)
                        .label("oldest_input_schema_id"),
                        func.last_value(Input.schema_id)
                        .over(partition_by=input_partition_by, order_by=input_order_by)
                        .label("newest_input_schema_id"),
                    )
                    .join(Input, Output.run_id == Input.run_id)
                    .where(*where)
                    .cte()
                )
            case "DOWNSTREAM":
                # dasasets as Inputs
                where = [
                    Input.created_at >= since,
                    Output.created_at >= since,
                    Input.dataset_id == any_(list(dataset_ids)),  # type: ignore[arg-type]
                ]
                if until:
                    where.extend([Input.created_at <= until, Output.created_at <= until])

                base_query = (
                    select(
                        Input.dataset_id.label("in_dataset_id"),
                        Input.created_at.label("created_at"),
                        Output.dataset_id.label("out_dataset_id"),
                        func.first_value(Output.schema_id)
                        .over(partition_by=output_partition_by, order_by=output_order_by)
                        .label("oldest_output_schema_id"),
                        func.last_value(Output.schema_id)
                        .over(partition_by=output_partition_by, order_by=output_order_by)
                        .label("newest_output_schema_id"),
                        func.first_value(Input.schema_id)
                        .over(partition_by=input_partition_by, order_by=input_order_by)
                        .label("oldest_input_schema_id"),
                        func.last_value(Input.schema_id)
                        .over(partition_by=input_partition_by, order_by=input_order_by)
                        .label("newest_input_schema_id"),
                    )
                    .join(Output, Input.run_id == Output.run_id)
                    .where(*where)
                    .cte()
                )
        query = select(
            func.max(base_query.c.created_at).label("created_at"),
            base_query.c.in_dataset_id,
            base_query.c.out_dataset_id,
            func.min(base_query.c.oldest_output_schema_id).label("min_output_schema_id"),
            func.max(base_query.c.newest_output_schema_id).label("max_output_schema_id"),
            func.min(base_query.c.oldest_input_schema_id).label("min_input_schema_id"),
            func.max(base_query.c.newest_input_schema_id).label("max_input_schema_id"),
        ).group_by(
            base_query.c.in_dataset_id,
            base_query.c.out_dataset_id,
        )

        query_result = await self._session.execute(query)
        results = []
        for row in query_result.all():
            output_schema_relevance_type: Literal["EXACT_MATCH", "LATEST_KNOWN"] | None
            input_schema_relevance_type: Literal["EXACT_MATCH", "LATEST_KNOWN"] | None
            if row.max_output_schema_id:
                output_schema_relevance_type = (
                    "EXACT_MATCH" if row.min_output_schema_id == row.max_output_schema_id else "LATEST_KNOWN"
                )
            else:
                output_schema_relevance_type = None

            if row.max_input_schema_id:
                input_schema_relevance_type = (
                    "EXACT_MATCH" if row.min_input_schema_id == row.max_input_schema_id else "LATEST_KNOWN"
                )
            else:
                input_schema_relevance_type = None
            results.append(
                IODatasetRelationRow(
                    created_at=row.created_at,
                    in_dataset_id=row.in_dataset_id,
                    out_dataset_id=row.out_dataset_id,
                    output_schema_id=row.max_output_schema_id,
                    output_schema_relevance_type=output_schema_relevance_type,
                    input_schema_id=row.max_input_schema_id,
                    input_schema_relevance_type=input_schema_relevance_type,
                ),
            )
        return results
