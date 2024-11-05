# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime
from enum import IntEnum
from typing import Literal

from fastapi import Query
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_serializer,
    field_validator,
    model_validator,
)

from data_rentgen.server.schemas.v1.pagination import PaginateQueryV1
from data_rentgen.utils import UUID


class OperationStatusV1(IntEnum):
    UNKNOWN = -1
    """No data about status"""

    STARTED = 0
    """Received START event"""

    SUCCEEDED = 1
    """Finished successfully"""

    FAILED = 2
    """Internal failure"""

    KILLED = 3
    """Killed externally, e.g. by user request or in case of OOM"""

    def __str__(self) -> str:
        return self.name


class OperationResponseV1(BaseModel):
    """Operation response."""

    kind: Literal["OPERATION"] = "OPERATION"
    id: UUID = Field(description="Operation id")
    created_at: datetime = Field(description="Operation creation time")
    run_id: UUID = Field(description="Run operation belongs to")
    status: OperationStatusV1 = Field(description="Operation status")
    name: str = Field(description="Operation name")
    type: str = Field(description="Operation type")

    position: int | None = Field(description="Sequentinal position of operation within the run, e.g. Spark jobId")
    group: str | None = Field(description="Operation group, e.g. Spark jobGroup")
    description: str | None = Field(description="Operation description, e.g. Spark jobDescription")

    started_at: datetime | None = Field(description="Start time of the Operation")
    ended_at: datetime | None = Field(description="End time of the Operation")

    model_config = ConfigDict(from_attributes=True)

    @field_serializer("status", when_used="json-unless-none")
    def _serialize_status(self, value: OperationStatusV1) -> str:
        return str(value)


class OperationQueryV1(PaginateQueryV1):
    """Query params for Operations paginate request."""

    since: datetime | None = Field(
        Query(
            default=None,
            description="Minimum value of Operation 'created_at' field, in ISO 8601 format",
            examples=["2008-09-15T15:53:00+05:00"],
        ),
    )
    until: datetime | None = Field(
        Query(
            default=None,
            description="Maximum value of Operation 'created_at' field, in ISO 8601 format",
            examples=["2008-09-15T15:53:00+05:00"],
        ),
    )
    operation_id: list[UUID] = Field(
        Query(
            default_factory=list,
            description="Operation ids, for exact match",
        ),
    )
    run_id: UUID | None = Field(
        Query(
            default=None,
            description="Run id, can be used only with 'since'",
        ),
    )

    model_config = ConfigDict(extra="forbid")

    @field_validator("until", mode="after")
    @classmethod
    def _check_until(cls, value: datetime | None, info: ValidationInfo) -> datetime | None:
        since = info.data.get("since")
        if since and value and since >= value:
            raise ValueError("'since' should be less than 'until'")
        return value

    @model_validator(mode="after")
    def _check_fields(self):
        if not any([self.operation_id, self.run_id]):
            raise ValueError("input should contain either 'run_id' or 'operation_id' field")
        if self.run_id and not self.since:
            raise ValueError("'run_id' can be passed only with 'since'")
        return self
