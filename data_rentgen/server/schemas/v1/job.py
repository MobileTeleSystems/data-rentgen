# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from fastapi import Query
from pydantic import BaseModel, Field

from data_rentgen.server.schemas.v1.pagination import PaginateQueryV1


class JobResponseV1(BaseModel):
    """Job response"""

    id: int = Field(description="Job id")
    location_id: int = Field(description="Id of corresponding Location")
    name: str = Field(description="Job name")

    class Config:
        from_attributes = True


class JobPaginateQueryV1(PaginateQueryV1):
    """Query params for Jobs paginate request."""

    job_id: list[int] = Field(Query(min_length=1), description="Job id")
