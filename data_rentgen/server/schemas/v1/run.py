# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime

from fastapi import Query
from pydantic import BaseModel, ConfigDict, Field

from data_rentgen.server.schemas.v1.pagination import PaginateQueryV1
from data_rentgen.server.schemas.v1.user import UserResponseV1
from data_rentgen.utils import UUID


class RunResponseV1(BaseModel):
    """Run response"""

    id: UUID = Field(description="Run id")
    job_id: int = Field(description="Job the run is associated with")
    parent_run_id: UUID | None = Field(description="Parent of current run", default=None)
    status: str = Field(description="Status")
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

    model_config = ConfigDict(
        from_attributes=True,
    )


class RunsByIdQueryV1(PaginateQueryV1):
    """Basic class with pagination query params."""

    run_id: list[UUID] = Field(Query(min_length=1, description="Run id"))


class RunsByJobQueryV1(PaginateQueryV1):
    """Class with filtering query params by time."""

    since: datetime = Field(
        Query(description="Start time of the Run in ISO 8601 format", example="2008-09-15T15:53:00+05:00"),
    )
    until: datetime | None = Field(
        Query(default=None, description="End time of the Run in ISO 8601 format", example="2008-09-15T15:53:00+05:00"),
    )
    job_id: int = Field(Query(description="Job id"))
