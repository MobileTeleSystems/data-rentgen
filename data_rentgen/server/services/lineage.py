# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime
from typing import Annotated

from fastapi import Depends

from data_rentgen.server.schemas.v1.lineage import (
    LineageEntityKind,
    LineageGranularity,
    LineageResponseV1,
)
from data_rentgen.server.services.lineage_strategies import (
    DatasetStrategy,
    JobStrategy,
    OperationStrategy,
    RunStrategy,
)
from data_rentgen.services.uow import UnitOfWork
from data_rentgen.utils import UUID


class LineageService:
    def __init__(self, uow: Annotated[UnitOfWork, Depends()]) -> None:
        self._uow = uow

    async def get_lineage(
        self,
        point_kind: LineageEntityKind,
        point_id: int | UUID,
        granularity: LineageGranularity,
        direction: str,
        depth: int,
        since: datetime,
        until: datetime | None,
    ) -> LineageResponseV1:
        if point_kind == LineageEntityKind.OPERATION:
            strategy = OperationStrategy(self._uow)
            return await strategy.get_lineage(point_id, granularity, direction, depth, since, until)
        elif point_kind == LineageEntityKind.DATASET:
            strategy = DatasetStrategy(self._uow)  # type: ignore[assignment]
            return await strategy.get_lineage(point_id, granularity, direction, depth, since, until)
        elif point_kind == LineageEntityKind.RUN:
            strategy = RunStrategy(self._uow)  # type: ignore[assignment]
            return await strategy.get_lineage(point_id, granularity, direction, depth, since, until)
        elif point_kind == LineageEntityKind.JOB:
            strategy = JobStrategy(self._uow)  # type: ignore[assignment]
            return await strategy.get_lineage(point_id, granularity, direction, depth, since, until)
        raise ValueError(f"Can't get lineage for this start point kind: {point_kind}")
