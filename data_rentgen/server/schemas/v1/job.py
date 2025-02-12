# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Literal

from fastapi import Query
from pydantic import BaseModel, ConfigDict, Field

from data_rentgen.server.schemas.v1.location import LocationResponseV1
from data_rentgen.server.schemas.v1.pagination import PaginateQueryV1


class JobResponseV1(BaseModel):
    """Job response"""

    kind: Literal["JOB"] = "JOB"
    id: int = Field(description="Job id")
    location: LocationResponseV1 = Field(description="Corresponding Location")
    name: str = Field(description="Job name")
    type: str = Field(description="Job type")

    model_config = ConfigDict(from_attributes=True)


class JobDetailedResponseV1(BaseModel):
    data: JobResponseV1 = Field(description="Job data")

    model_config = ConfigDict(from_attributes=True)


class JobPaginateQueryV1(PaginateQueryV1):
    """Query params for Jobs paginate request."""

    job_id: list[int] = Field(Query(default_factory=list), description="Job id")
    search_query: str | None = Field(
        Query(
            default=None,
            min_length=3,
            description="Search query",
        ),
    )

    model_config = ConfigDict(extra="forbid")
