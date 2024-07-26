# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime

from fastapi import Query
from pydantic import BaseModel, ConfigDict, Field

from data_rentgen.server.schemas.v1.pagination import PaginateQueryV1
from data_rentgen.utils import UUID


class OperationResponseV1(BaseModel):
    """Operation response."""

    id: UUID = Field(description="Operation id")
    run_id: UUID = Field(description="Run operation is a part of")
    status: str = Field(description="Operation status")
    name: str = Field(description="Operation name")
    type: str = Field(description="Operation type")

    position: int | None = Field(description="Sequentinal position of operation within the run, e.g. Spark jobId")
    description: str | None = Field(description="Operation description")

    started_at: datetime | None = Field(description="Start time of the Operation")
    ended_at: datetime | None = Field(description="End time of the Operation")

    model_config = ConfigDict(from_attributes=True)


class OperationByIdQueryV1(PaginateQueryV1):
    """Basic class with pagination query params."""

    operation_id: list[UUID] = Field(Query(min_length=1, description="Operation id"))


class OperationByRunQueryV1(PaginateQueryV1):
    """Class with filtering query params by time."""

    since: datetime = Field(
        Query(description="Start time of the Run in ISO 8601 format", example="2008-09-15T15:53:00+05:00"),
    )
    until: datetime | None = Field(
        Query(default=None, description="End time of the Run in ISO 8601 format", example="2008-09-15T15:53:00+05:00"),
    )
    run_id: UUID = Field(Query(description="Run id"))
