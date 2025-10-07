# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from data_rentgen.server.schemas.v1.location import LocationResponseV1
from data_rentgen.server.schemas.v1.pagination import PaginateQueryV1


class JobResponseV1(BaseModel):
    """Job response"""

    id: str = Field(description="Job id", coerce_numbers_to_str=True)
    location: LocationResponseV1 = Field(description="Corresponding Location")
    name: str = Field(description="Job name")
    type: str = Field(description="Job type")

    model_config = ConfigDict(from_attributes=True)


class JobDetailedResponseV1(BaseModel):
    id: str = Field(description="Job id", coerce_numbers_to_str=True)
    data: JobResponseV1 = Field(description="Job data")

    model_config = ConfigDict(from_attributes=True)


class JobTypesResponseV1(BaseModel):
    """Job types"""

    job_types: list[str] = Field(description="List of distinct job types")

    model_config = ConfigDict(from_attributes=True)


class JobPaginateQueryV1(PaginateQueryV1):
    """Query params for Jobs paginate request."""

    job_id: list[int] = Field(default_factory=list, description="Job id")
    search_query: str | None = Field(
        default=None,
        min_length=3,
        description="Search query",
    )
    job_type: list[str] = Field(
        default_factory=list,
        description="Specify job types",
    )
    location_id: int | None = Field(
        default=None,
        description="The location id which jobs belong",
    )

    model_config = ConfigDict(extra="forbid")
