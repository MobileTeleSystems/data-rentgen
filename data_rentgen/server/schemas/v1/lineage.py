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


class LineageEntityKindV1(str, Enum):
    JOB = "JOB"
    RUN = "RUN"
    OPERATION = "OPERATION"
    DATASET = "DATASET"

    def __str__(self) -> str:
        return self.value


class LineageDirectionV1(str, Enum):
    FROM = "FROM"
    TO = "TO"

    def __str__(self) -> str:
        return self.value

    def __invert__(self) -> "LineageDirectionV1":
        return LineageDirectionV1.TO if self == LineageDirectionV1.FROM else LineageDirectionV1.FROM


class LineageRelationKindV1(str, Enum):
    PARENT = "PARENT"
    INTERACTION = "INTERACTION"

    def __str__(self) -> str:
        return self.value


class LineageEntityV1(BaseModel):
    kind: LineageEntityKindV1 = Field(description="Type of Lineage entity")
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
    point_kind: LineageEntityKindV1 = Field(
        Query(description="Type of the Lineage start point", examples=["job"]),
    )
    point_id: int | UUID = Field(
        Query(
            description="Id of the Lineage start point",
            examples=[42, "01913217-b761-7b1a-bb52-489da9c8b9c8"],
        ),
    )
    direction: LineageDirectionV1 = Field(
        Query(description="Direction of the lineage", examples=["from"]),
    )
    depth: int = Field(
        Query(
            default=1,
            ge=1,
            le=3,
            description="Depth of the lineage",
            examples=[1, 2, 3],
        ),
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

        if self.point_kind in {LineageEntityKindV1.JOB, LineageEntityKindV1.DATASET} and not isinstance(
            self.point_id,
            int,
        ):
            raise ValueError(f"'point_id' should be int for '{self.point_kind}' kind")
        elif self.point_kind in {LineageEntityKindV1.RUN, LineageEntityKindV1.OPERATION} and not isinstance(
            self.point_id,
            OLD_UUID,
        ):
            raise ValueError(f"'point_id' should be UUIDv7 for '{self.point_kind}' kind")
        return self


class LineageRelationv1(BaseModel):
    kind: LineageRelationKindV1 = Field(description="Kind of relation", examples=["PARENT", "INTERACTION"])
    from_: LineageEntityV1 = Field(description="Start point of relation", serialization_alias="from")
    to: LineageEntityV1 = Field(description="End point of relation")
    type: str | None = Field(description="Type of interaction", examples=["READ", "APPEND"], default=None)


class LineageResponseV1(BaseModel):
    relations: list[LineageRelationv1] = []
    nodes: list[RunResponseV1 | OperationResponseV1 | JobResponseV1 | DatasetResponseV1] = []
