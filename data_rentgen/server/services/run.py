# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import Depends
from sqlalchemy import Row

from data_rentgen.db.models.run import Run
from data_rentgen.dto.pagination import PaginationDTO
from data_rentgen.services.uow import UnitOfWork


@dataclass
class RunServiceIOStatistics:
    total_datasets: int = 0
    total_bytes: int = 0
    total_rows: int = 0
    total_files: int = 0

    @classmethod
    def from_row(cls, row: Row | None):
        if not row:
            return cls()

        return cls(
            total_datasets=row.total_datasets,
            total_bytes=row.total_bytes or 0,
            total_rows=row.total_rows or 0,
            total_files=row.total_files or 0,
        )


@dataclass
class RunServiceOperationStatistics:
    total_operations: int = 0

    @classmethod
    def from_row(cls, row: Row | None):
        if not row:
            return cls()

        return cls(
            total_operations=row.total_operations,
        )


@dataclass
class RunServiceStatistics:
    inputs: RunServiceIOStatistics
    outputs: RunServiceIOStatistics
    operations: RunServiceOperationStatistics


@dataclass
class RunServicePageItem:
    id: UUID
    data: Run
    statistics: RunServiceStatistics


class RunServicePaginatedResult(PaginationDTO[RunServicePageItem]):
    pass


class RunService:
    def __init__(self, uow: Annotated[UnitOfWork, Depends()]):
        self._uow = uow

    async def paginate(
        self,
        page: int,
        page_size: int,
        since: datetime | None,
        until: datetime | None,
        run_ids: Collection[UUID],
        job_id: int | None,
        parent_run_id: UUID | None,
        search_query: str | None,
        job_types: Collection[str],
        statuses: Collection[str],
    ) -> RunServicePaginatedResult:
        pagination = await self._uow.run.paginate(
            page=page,
            page_size=page_size,
            since=since,
            until=until,
            run_ids=run_ids,
            job_id=job_id,
            parent_run_id=parent_run_id,
            search_query=search_query,
            job_types=job_types,
            statuses=statuses,
        )
        run_ids = [item.id for item in pagination.items]
        input_stats = await self._uow.input.get_stats_by_run_ids(run_ids)
        output_stats = await self._uow.output.get_stats_by_run_ids(run_ids)
        operation_stats = await self._uow.operation.get_stats_by_run_ids(run_ids)

        return RunServicePaginatedResult(
            page=pagination.page,
            page_size=pagination.page_size,
            total_count=pagination.total_count,
            items=[
                RunServicePageItem(
                    id=run.id,
                    data=run,
                    statistics=RunServiceStatistics(
                        inputs=RunServiceIOStatistics.from_row(input_stats.get(run.id)),
                        outputs=RunServiceIOStatistics.from_row(output_stats.get(run.id)),
                        operations=RunServiceOperationStatistics.from_row(operation_stats.get(run.id)),
                    ),
                )
                for run in pagination.items
            ],
        )
