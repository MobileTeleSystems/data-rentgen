# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime
from enum import IntEnum
from uuid import UUID

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
from data_rentgen.server.schemas.v1.user import UserResponseV1
from data_rentgen.utils import UUIDv6Plus


class RunStatusV1(IntEnum):
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


class RunResponseV1(BaseModel):
    """Run response"""

    id: UUID = Field(description="Run id")
    created_at: datetime = Field(description="Run creation time")
    job_id: str = Field(description="Job the run is associated with", coerce_numbers_to_str=True)
    parent_run_id: UUID | None = Field(description="Parent of current run", default=None)
    status: RunStatusV1 = Field(description="Run status")
    external_id: str | None = Field(description="External id, e.g. Spark applicationid", default=None)
    attempt: str | None = Field(description="Attempt number of the run", default=None)
    persistent_log_url: str | None = Field(
        description="Persistent log url of the run, like Spark history server url, optional",
        default=None,
    )
    running_log_url: str | None = Field(
        description="Log url of the run in progress, like Spark session UI url, optional",
        default=None,
    )
    started_at: datetime | None = Field(description="Start time of the Run", default=None)
    started_by_user: UserResponseV1 | None = Field(description="User who started the Run", default=None)
    start_reason: str | None = Field(description="Start reason of the Run", default=None)
    ended_at: datetime | None = Field(description="End time of the Run", default=None)
    end_reason: str | None = Field(description="End reason of the Run", default=None)

    model_config = ConfigDict(from_attributes=True)

    @field_serializer("status", when_used="json-unless-none")
    def _serialize_status(self, value: RunStatusV1) -> str:
        return str(value)


class RunIOStatisticsReponseV1(BaseModel):
    """Run IO statistics response."""

    total_datasets: int = Field(default=0, description="Total number of datasets")
    total_bytes: int = Field(default=0, description="Total number of bytes")
    total_rows: int = Field(default=0, description="Total number of rows")
    total_files: int = Field(default=0, description="Total number of files")

    model_config = ConfigDict(from_attributes=True)


class RunOperationStatisticsReponseV1(BaseModel):
    """Run operation statistics response."""

    total_operations: int = Field(default=0, description="Total number of operations")

    model_config = ConfigDict(from_attributes=True)


class RunStatisticsReponseV1(BaseModel):
    """Run statistics response."""

    outputs: RunIOStatisticsReponseV1 = Field(description="Output statistics")
    inputs: RunIOStatisticsReponseV1 = Field(description="Input statistics")
    operations: RunOperationStatisticsReponseV1 = Field(description="Operation statistics")

    model_config = ConfigDict(from_attributes=True)


class RunDetailedResponseV1(BaseModel):
    """Run response."""

    id: UUID = Field(description="Run id")
    data: RunResponseV1 = Field(description="Run data")
    statistics: RunStatisticsReponseV1 = Field(description="Run statistics")

    model_config = ConfigDict(from_attributes=True)


class RunsQueryV1(PaginateQueryV1):
    """Query params for Runs paginate request."""

    since: datetime | None = Field(
        Query(
            default=None,
            description="Minimum value of Run 'created_at' field, in ISO 8601 format",
            examples=["2008-09-15T15:53:00+05:00"],
        ),
    )
    until: datetime | None = Field(
        Query(
            default=None,
            description="Maximum value of Run 'created_at' field, in ISO 8601 format",
            examples=["2008-09-15T15:53:00+05:00"],
        ),
    )
    run_id: list[UUIDv6Plus] = Field(
        Query(
            default_factory=list,
            description="Run ids, for exact match",
        ),
    )
    job_id: int | None = Field(
        Query(
            default=None,
            description="Job id, can be used only with 'since'",
        ),
    )

    parent_run_id: UUIDv6Plus | None = Field(
        Query(
            default=None,
            description="Parent run id, can be used only with 'since' and 'until'",
            examples=["01913217-b761-7b1a-bb52-489da9c8b9c8"],
        ),
    )

    search_query: str | None = Field(
        Query(
            default=None,
            min_length=3,
            description="Search query",
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
    def _check_fields(self):  # noqa: WPS238
        if not any([self.run_id, self.job_id, self.parent_run_id, self.search_query]):
            raise ValueError(
                "input should contain either 'run_id', 'job_id', 'parent_run_id' or 'search_query' field",
            )
        if self.job_id and not self.since:
            raise ValueError("'job_id' can be passed only with 'since'")
        if self.parent_run_id and not self.since:
            raise ValueError("'parent_run_id' can be passed only with 'since'")
        if self.search_query and not self.since:
            raise ValueError("'search_query' can be passed only with 'since'")
        return self
