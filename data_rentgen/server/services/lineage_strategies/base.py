# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod
from datetime import datetime

from data_rentgen.server.schemas.v1.lineage import LineageEntityKind, LineageGranularity
from data_rentgen.services.uow import UnitOfWork
from data_rentgen.utils import UUID


class AbstractStrategy(ABC):
    point_kind: LineageEntityKind

    def __init__(
        self,
        uow: UnitOfWork,
    ):
        self._uow = uow

    @abstractmethod
    async def get_lineage(
        self,
        point_id: int | UUID,
        granularity: LineageGranularity,
        direction: str,
        depth: int,
        since: datetime,
        until: datetime | None,
    ):
        raise NotImplementedError

    async def _check_granularity(self, granularity: LineageGranularity) -> None | LineageGranularity:
        """
        check if granularity is more or equal to point_kind
            example:
                granularity: JOB
                point_kind: RUN
                In this case, we can't get lineage.
                Granularity less then point_kind.
                JOB.to_int() < RUN.to_int()
        """
        if granularity.to_int() < self.point_kind.to_int():
            raise ValueError(f"Granularity {granularity} is less than {self.point_kind}")

        return granularity

    @classmethod
    async def _get_direction(cls, direction: str) -> list[str]:
        if direction == "from":
            return ["ALTER", "APPEND", "CREATE", "DROP", "OVERWRITE", "RENAME", "TRUNCATE"]
        elif direction == "to":
            return ["READ"]
        raise ValueError(f"No such direction: {direction}")
