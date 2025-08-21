# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from data_rentgen.server.schemas.v1.address import AddressResponseV1
from data_rentgen.server.schemas.v1.pagination import PaginateQueryV1


class LocationResponseV1(BaseModel):
    id: str = Field(description="Location id", coerce_numbers_to_str=True)
    type: str = Field(description="Location type, e.g kafka, hdfs, postgres")
    name: str = Field(description="Location name, e.g. cluster name")
    addresses: list[AddressResponseV1] = Field(description="List of addresses")
    external_id: str | None = Field(description="External ID for integration with other systems")

    model_config = ConfigDict(from_attributes=True)


class LocationDatasetStatisticsReponseV1(BaseModel):
    """Location dataset statistics response."""

    total_datasets: int = Field(description="Total number of datasets bound to this location")

    model_config = ConfigDict(from_attributes=True)


class LocationJobStatisticsReponseV1(BaseModel):
    """Location job statistics response."""

    total_jobs: int = Field(description="Total number of jobs bound to this location")

    model_config = ConfigDict(from_attributes=True)


class LocationStatisticsReponseV1(BaseModel):
    """Location statistics response."""

    datasets: LocationDatasetStatisticsReponseV1 = Field(description="Dataset statistics")
    jobs: LocationJobStatisticsReponseV1 = Field(description="Dataset statistics")

    model_config = ConfigDict(from_attributes=True)


class LocationDetailedResponseV1(BaseModel):
    id: str = Field(description="Location id", coerce_numbers_to_str=True)
    data: LocationResponseV1 = Field(description="Location data")
    statistics: LocationStatisticsReponseV1 = Field(description="Location statistics")

    model_config = ConfigDict(from_attributes=True)


class LocationPaginateQueryV1(PaginateQueryV1):
    """Query params for Location paginate request."""

    location_id: list[int] = Field(default_factory=list, description="Location id")
    location_type: str | None = Field(default=None, description="Location type")
    search_query: str | None = Field(
        default=None,
        min_length=3,
        description="Search query",
    )

    model_config = ConfigDict(extra="forbid")


class UpdateLocationRequestV1(BaseModel):
    external_id: str | None = Field(description="External ID for integration with other systems")

    model_config = ConfigDict(extra="forbid")
