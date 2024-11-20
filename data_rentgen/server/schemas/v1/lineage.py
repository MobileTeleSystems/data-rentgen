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
        default=LineageDirectionV1.BOTH,
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
        default="RUN",
        description="Granularity of the dataset lineage",
        examples=["OPERATION", "RUN", "JOB"],
    )

    model_config = ConfigDict(extra="forbid")


class JobLineageQueryV1(BaseLineageQueryV1):
    start_node_id: int = Field(description="Job id", examples=[42])
    granularity: Literal["JOB", "RUN"] = Field(
        default="JOB",
        description="Granularity of the job lineage",
        examples=["JOB", "RUN"],
    )


class OperationLineageQueryV1(BaseLineageQueryV1):
    start_node_id: UUID = Field(description="Operation id", examples=["00000000-0000-0000-0000-000000000000"])


class RunLineageQueryV1(BaseLineageQueryV1):
    start_node_id: UUID = Field(description="Run id", examples=["00000000-0000-0000-0000-000000000000"])
    granularity: Literal["OPERATION", "RUN"] = Field(
        default="RUN",
        description="Granularity of the run lineage",
        examples=["OPERATION", "RUN"],
    )


class LineageParentRelationV1(BaseModel):
    kind: Literal["PARENT"] = "PARENT"
    from_: LineageEntityV1 = Field(description="Start point of relation", serialization_alias="from")
    to: LineageEntityV1 = Field(description="End point of relation")


class LineageOutputRelationSchemaFieldV1(BaseModel):
    name: str
    type: str | None = Field(default=None)
    description: str | None = Field(default=None)
    fields: list["LineageOutputRelationSchemaFieldV1"] = Field(description="Nested fields", default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class LineageOutputRelationSchemaV1(BaseModel):
    fields: list[LineageOutputRelationSchemaFieldV1] = Field(description="Schema fields")

    model_config = ConfigDict(from_attributes=True)


class LineageInputRelationV1(BaseModel):
    kind: Literal["INPUT"] = "INPUT"
    from_: LineageEntityV1 = Field(description="Start point of relation", serialization_alias="from")
    to: LineageEntityV1 = Field(description="End point of relation")
    last_interaction_at: datetime = Field(description="Last interaction at", examples=["2008-09-15T15:53:00+05:00"])
    num_bytes: int | None = Field(description="Number of bytes", examples=[42], default=None)
    num_rows: int | None = Field(description="Number of rows", examples=[42], default=None)
    num_files: int | None = Field(description="Number of files", examples=[42], default=None)
    i_schema: LineageOutputRelationSchemaV1 | None = Field(
        description="Schema",
        default=None,
        serialization_alias="schema",
    )


class LineageOutputRelationV1(BaseModel):
    kind: Literal["OUTPUT"] = "OUTPUT"
    from_: LineageEntityV1 = Field(description="Start point of relation", serialization_alias="from")
    to: LineageEntityV1 = Field(description="End point of relation")
    type: str = Field(description="Type of relation", examples=["CREATE", "APPEND"])
    last_interaction_at: datetime = Field(description="Last interaction at", examples=["2008-09-15T15:53:00+05:00"])
    num_bytes: int | None = Field(description="Number of bytes", examples=[42], default=None)
    num_rows: int | None = Field(description="Number of rows", examples=[42], default=None)
    num_files: int | None = Field(description="Number of files", examples=[42], default=None)
    o_schema: LineageOutputRelationSchemaV1 | None = Field(
        description="Schema",
        default=None,
        serialization_alias="schema",
    )


class LineageSymlinkRelationV1(BaseModel):
    kind: Literal["SYMLINK"] = "SYMLINK"
    from_: LineageEntityV1 = Field(description="Start point of relation", serialization_alias="from")
    to: LineageEntityV1 = Field(description="End point of relation")
    type: str = Field(description="Type of relation between datasets", examples=["METASTORE", "WAREHOUSE"])


class LineageResponseV1(BaseModel):
    relations: list[
        LineageParentRelationV1 | LineageInputRelationV1 | LineageOutputRelationV1 | LineageSymlinkRelationV1
    ] = Field(description="List of relations", default_factory=list)
    nodes: list[RunResponseV1 | OperationResponseV1 | JobResponseV1 | DatasetResponseV1] = Field(
        description="List of nodes",
        default_factory=list,
    )
