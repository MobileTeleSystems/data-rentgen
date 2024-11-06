# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from fastapi import Query
from pydantic import BaseModel, ConfigDict, Field

from data_rentgen.server.schemas.v1.address import AddressResponseV1
from data_rentgen.server.schemas.v1.pagination import PaginateQueryV1


class LocationResponseV1(BaseModel):
    id: int = Field(description="Location id")
    type: str = Field(description="Location type, e.g kafka, hdfs, postgres")
    name: str = Field(description="Location name, e.g. cluster name")
    addresses: list[AddressResponseV1] = Field(description="List of addresses")
    external_id: str | None = Field(description="External ID for integration with other systems")

    model_config = ConfigDict(from_attributes=True)


class LocationPaginateQueryV1(PaginateQueryV1):
    """Query params for Location paginate request."""

    location_id: list[int] = Field(Query(default_factory=list), description="Location id")
    location_type: str | None = Field(Query(default=None), description="Location type")
    search_query: str | None = Field(
        Query(
            default=None,
            min_length=3,
            description="Search query",
        ),
    )

    model_config = ConfigDict(extra="forbid")


class UpdateLocationRequestV1(BaseModel):
    external_id: str | None = Field(description="External ID for integration with other systems")

    model_config = ConfigDict(extra="forbid")
