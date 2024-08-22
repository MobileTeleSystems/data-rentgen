# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime
from enum import Enum

from fastapi import Query
from pydantic import BaseModel, ConfigDict, Field

from data_rentgen.server.schemas.v1.dataset import DatasetResponseV1
from data_rentgen.server.schemas.v1.job import JobResponseV1
from data_rentgen.server.schemas.v1.operation import OperationResponseV1
from data_rentgen.server.schemas.v1.run import RunResponseV1
from data_rentgen.utils import UUID


class LineageEntityKind(str, Enum):
    JOB = "job"
    RUN = "run"
    OPERATION = "operation"
    DATASET = "dataset"

    def to_int(self) -> int:
        int_map = {
            LineageEntityKind.DATASET: 0,
            LineageEntityKind.JOB: 1,
            LineageEntityKind.RUN: 2,
            LineageEntityKind.OPERATION: 3,
        }
        return int_map[self]


class LineageDirection(str, Enum):
    FROM = "from"
    TO = "to"


class LineageGranularity(str, Enum):
    JOB = "job"
    RUN = "run"
    OPERATION = "operation"

    def to_int(self) -> int:
        int_map = {
            LineageGranularity.JOB: 1,
            LineageGranularity.RUN: 2,
            LineageGranularity.OPERATION: 3,
        }
        return int_map[self]


class LineageEntity(BaseModel):
    kind: LineageEntityKind = Field(description="Type of Lineage entity")
    id: int | UUID = Field(description="Id of Lineage entity")

    model_config = ConfigDict(from_attributes=True, use_enum_values=True)


# TODO: Maybe add default values for all fields except since and until?
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
    granularity: LineageGranularity = Field(
        Query(description="Granularity of the lineage", examples=["job"]),
    )
    depth: int = Field(Query(description="Depth of the lineage", examples=["2"], le=3, default=1))


class LineageRelation(BaseModel):
    kind: str = Field(description="Kind of relation")
    from_: LineageEntity = Field(description="Start point of relation")
    to: LineageEntity = Field(description="End point of relation")


class LineageResponseV1(BaseModel):
    relations: list[LineageRelation] = []
    nodes: list[RunResponseV1 | OperationResponseV1 | JobResponseV1 | DatasetResponseV1] = []
