# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime
from typing import Annotated

from fastapi import Depends

from data_rentgen.server.schemas.v1.lineage import LineageEntityKind, LineageResponseV1
from data_rentgen.server.services.lineage_strategies import (
    AbstractStrategy,
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
        direction: str,
        since: datetime,
        until: datetime | None,
    ) -> LineageResponseV1:
        # TODO: Remove Response schemas from LineageService and Strategies
        # TODO: Add depths logic
        # TODO: Add granularity logic
        # TODO: Add Child runs logic
        strategy: AbstractStrategy
        match point_kind:
            case LineageEntityKind.OPERATION:
                strategy = OperationStrategy(self._uow)
            case LineageEntityKind.DATASET:
                strategy = DatasetStrategy(self._uow)
            case LineageEntityKind.RUN:
                strategy = RunStrategy(self._uow)
            case LineageEntityKind.JOB:
                strategy = JobStrategy(self._uow)
            case _:
                raise ValueError(f"Can't get lineage for this start point kind: {point_kind}")

        return await strategy.get_lineage(point_id, direction, since, until)
