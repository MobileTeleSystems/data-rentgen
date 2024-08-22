# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime
from typing import Literal

from fastapi import Query
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from data_rentgen.server.schemas.v1.pagination import PaginateQueryV1
from data_rentgen.server.schemas.v1.user import UserResponseV1
from data_rentgen.utils import UUID


class RunResponseV1(BaseModel):
    """Run response"""

    kind: Literal["RUN"] = "RUN"
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


class RunsQueryV1(PaginateQueryV1):
    """Query params for Runs paginate request."""

    run_id: list[UUID] = Field(
        Query(
            default_factory=list,
            description="Run ids, for exact match",
        ),
    )
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
    job_id: int | None = Field(
        Query(
            default=None,
            description="Job id, can be used only with 'since'",
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
    def _check_fields(self):
        if self.run_id:
            if self.job_id or self.since or self.until:
                raise ValueError("fields 'job_id','since', 'until' cannot be used if 'run_id' is set")
        elif not self.job_id or not self.since:
            raise ValueError("input should contain either 'job_id' and 'since', or 'run_id' field")
        return self
