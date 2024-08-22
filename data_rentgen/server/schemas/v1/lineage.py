# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime
from enum import Enum

from fastapi import Query
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from data_rentgen.server.schemas.v1.dataset import DatasetResponseV1
from data_rentgen.server.schemas.v1.job import JobResponseV1
from data_rentgen.server.schemas.v1.operation import OperationResponseV1
from data_rentgen.server.schemas.v1.run import RunResponseV1
from data_rentgen.utils import UUID


class LineageEntityKind(str, Enum):
    JOB = "JOB"
    RUN = "RUN"
    OPERATION = "OPERATION"
    DATASET = "DATASET"

    def __str__(self) -> str:
        return self.value


class LineageDirection(str, Enum):
    FROM = "from"
    TO = "to"

    def __str__(self) -> str:
        return self.value


class LineageEntity(BaseModel):
    kind: LineageEntityKind = Field(description="Type of Lineage entity")
    id: int | UUID = Field(description="Id of Lineage entity")

    model_config = ConfigDict(from_attributes=True, use_enum_values=True)


class LineageQueryV1(BaseModel):
    since: datetime = Field(
        Query(description="", examples=["2008-09-15T15:53:00+05:00"]),
    )
    until: datetime | None = Field(
        Query(
            default=None,
            description="",
            examples=["2008-09-15T15:53:00+05:00"],
        ),
    )
    point_kind: LineageEntityKind = Field(
        Query(description="Type of the Lineage start point", examples=["job"]),
    )
    point_id: int | UUID = Field(
        Query(description="Id of the Lineage start point"),
        examples=[42, "01913217-b761-7b1a-bb52-489da9c8b9c8"],
    )
    direction: LineageDirection = Field(
        Query(description="Direction of the lineage", examples=["from"]),
    )

    @field_validator("until", mode="after")
    @classmethod
    def _check_until(cls, value: datetime | None, info: ValidationInfo) -> datetime | None:
        since = info.data.get("since")
        if since and value and since >= value:
            raise ValueError("'since' should be less than 'until'")
        return value

    @model_validator(mode="after")
    def _check_ids(self):
        from uuid import UUID as OLD_UUID

        if self.point_kind in {LineageEntityKind.JOB, LineageEntityKind.DATASET} and not isinstance(self.point_id, int):
            raise ValueError(f"'point_id' should be int for '{self.point_kind}' kind")
        elif self.point_kind in {LineageEntityKind.RUN, LineageEntityKind.OPERATION} and not isinstance(
            self.point_id,
            OLD_UUID,
        ):
            raise ValueError(f"'point_id' should be UUIDv7 for '{self.point_kind}' kind")
        return self


class LineageRelation(BaseModel):
    kind: str = Field(description="Kind of relation")
    from_: LineageEntity = Field(description="Start point of relation", serialization_alias="from")
    to: LineageEntity = Field(description="End point of relation")
    type: str | None = Field(description="Type of interaction", examples=["READ", "APPEND"], default=None)


class LineageResponseV1(BaseModel):
    relations: list[LineageRelation] = []
    nodes: list[RunResponseV1 | OperationResponseV1 | JobResponseV1 | DatasetResponseV1] = []
