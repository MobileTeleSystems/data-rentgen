# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Literal

from fastapi import Query
from pydantic import BaseModel, ConfigDict, Field

from data_rentgen.server.schemas.v1.location import LocationResponseV1
from data_rentgen.server.schemas.v1.pagination import PaginateQueryV1


class DatasetResponseV1(BaseModel):
    kind: Literal["DATASET"] = "DATASET"
    id: int = Field(description="Dataset id")
    location: LocationResponseV1 = Field(description="Corresponding Location")
    name: str = Field(description="Dataset name")
    format: str | None = Field(description="Data format", default=None)

    model_config = ConfigDict(from_attributes=True)


class DatasetDetailedResponseV1(BaseModel):
    data: DatasetResponseV1 = Field(description="Dataset data")

    model_config = ConfigDict(from_attributes=True)


class DatasetPaginateQueryV1(PaginateQueryV1):
    """Query params for Dataset paginate request."""

    dataset_id: list[int] = Field(Query(default_factory=list), description="Dataset id")
    search_query: str | None = Field(
        Query(
            default=None,
            min_length=3,
            description="Search query",
        ),
    )

    model_config = ConfigDict(extra="forbid")
