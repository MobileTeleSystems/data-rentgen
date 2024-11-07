# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

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
    DOWNSTREAM = "DOWNSTREAM"
    UPSTREAM = "UPSTREAM"
    BOTH = "BOTH"

    def __str__(self) -> str:
        return self.value


class LineageEntityV1(BaseModel):
    kind: LineageEntityKindV1 = Field(description="Type of Lineage entity")
    id: int | UUID = Field(description="Id of Lineage entity")

    model_config = ConfigDict(from_attributes=True, use_enum_values=True)


class BaseLineageQueryV1(BaseModel):
    since: datetime = Field(
        description="",
        examples=["2008-09-15T15:53:00+05:00"],
    )
    until: datetime | None = Field(
        default=None,
        description="",
        examples=["2008-09-15T15:53:00+05:00"],
    )
    direction: LineageDirectionV1 = Field(
        description="Direction of the lineage",
        examples=["DOWNSTREAM", "UPSTREAM", "BOTH"],
    )
    depth: int = Field(
        default=1,
        ge=1,
        le=3,
        description="Depth of the lineage",
        examples=[1, 3],
    )

    model_config = ConfigDict(extra="forbid")

    @field_validator("until", mode="after")
    @classmethod
    def _check_until(cls, value: datetime | None, info: ValidationInfo) -> datetime | None:
        since = info.data.get("since")
        if since and value and since >= value:
            raise ValueError("'since' should be less than 'until'")
        return value


class DatasetLineageQueryV1(BaseLineageQueryV1):
    start_node_id: int = Field(description="Dataset id", examples=[42])
    granularity: Literal["OPERATION", "RUN", "JOB"] = Field(
        description="Granularity of the dataset lineage",
        default="RUN",
        examples=["OPERATION", "RUN", "JOB"],
    )

    model_config = ConfigDict(extra="forbid")


class JobLineageQueryV1(BaseLineageQueryV1):
    start_node_id: int = Field(description="Job id", examples=[42])
    granularity: Literal["JOB", "RUN"] = Field(
        description="Granularity of the job lineage",
        default="JOB",
        examples=["JOB", "RUN"],
    )


class OperationLineageQueryV1(BaseLineageQueryV1):
    start_node_id: UUID = Field(description="Operation id", examples=["00000000-0000-0000-0000-000000000000"])


class RunLineageQueryV1(BaseLineageQueryV1):
    start_node_id: UUID = Field(description="Run id", examples=["00000000-0000-0000-0000-000000000000"])
    granularity: Literal["OPERATION", "RUN"] = Field(
        description="Granularity of the run lineage",
        default="RUN",
        examples=["OPERATION", "RUN"],
    )


class LineageRelationV1(BaseModel):
    kind: Literal["PARENT", "INPUT", "OUTPUT", "SYMLINK"] = Field(description="Kind of relation")
    from_: LineageEntityV1 = Field(description="Start point of relation", serialization_alias="from")
    to: LineageEntityV1 = Field(description="End point of relation")


class LineageParrentRelationV1(LineageRelationV1):
    kind: Literal["PARENT"] = "PARENT"


class LineageInputRelationV1(LineageRelationV1):
    kind: Literal["INPUT"] = "INPUT"
    last_interaction_at: datetime = Field(description="Last interaction at", examples=["2008-09-15T15:53:00+05:00"])
    num_bytes: int | None = Field(description="Number of bytes", examples=[42], default=None)
    num_rows: int | None = Field(description="Number of rows", examples=[42], default=None)
    num_files: int | None = Field(description="Number of files", examples=[42], default=None)


class LineageOutputRelationV1(LineageRelationV1):
    kind: Literal["OUTPUT"] = "OUTPUT"
    type: str = Field(description="Type of relation", examples=["CREATE", "APPEND"])
    last_interaction_at: datetime = Field(description="Last interaction at", examples=["2008-09-15T15:53:00+05:00"])
    num_bytes: int | None = Field(description="Number of bytes", examples=[42], default=None)
    num_rows: int | None = Field(description="Number of rows", examples=[42], default=None)
    num_files: int | None = Field(description="Number of files", examples=[42], default=None)


class LineageSymlinkRelationV1(LineageRelationV1):
    kind: Literal["SYMLINK"] = "SYMLINK"
    type: str = Field(description="Type of relation between datasets", examples=["METASTORE", "WAREHOUSE"])


class LineageResponseV1(BaseModel):
    relations: list[
        LineageParrentRelationV1 | LineageInputRelationV1 | LineageOutputRelationV1 | LineageSymlinkRelationV1
    ] = []
    nodes: list[RunResponseV1 | OperationResponseV1 | JobResponseV1 | DatasetResponseV1] = []
