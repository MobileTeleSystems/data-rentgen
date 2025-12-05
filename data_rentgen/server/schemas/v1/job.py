# SPDX-FileCopyrightText: 2024-present MTS PJSC
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

    job_types: list[str] = Field(
        description="List of distinct job types",
        examples=[["SPARK_APPLICATION", "AIRFLOW_DAG"]],
    )

    model_config = ConfigDict(from_attributes=True)


class JobPaginateQueryV1(PaginateQueryV1):
    """Query params for Jobs paginate request."""

    job_id: list[int] = Field(
        default_factory=list,
        description="Ids of jobs to fetch specific items only",
    )
    search_query: str | None = Field(
        default=None,
        min_length=3,
        description="Search query, partial match by job name or location name/address",
        examples=["my job"],
    )
    job_type: list[str] = Field(
        default_factory=list,
        description="Types of jobs",
        examples=[["SPARK_APPLICATION", "AIRFLOW_DAG"]],
    )
    location_id: list[int] = Field(
        default_factory=list,
        description="Ids of locations the job started at",
        examples=[[123]],
    )
    location_type: list[str] = Field(
        default_factory=list,
        description="Types of location the job started at",
        examples=[["yarn"]],
    )

    model_config = ConfigDict(extra="forbid")
