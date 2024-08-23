# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod
from datetime import datetime

from data_rentgen.dto import InteractionTypeDTO
from data_rentgen.server.schemas.v1.lineage import LineageEntityKind
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
        direction: str,
        since: datetime,
        until: datetime | None,
    ):
        raise NotImplementedError

    @classmethod
    def _get_direction(cls, direction: str) -> list[str]:
        if direction == "FROM":
            return InteractionTypeDTO.write_interactions()
        elif direction == "TO":
            return [InteractionTypeDTO.READ.value]
        raise ValueError(f"No such direction: {direction}")
