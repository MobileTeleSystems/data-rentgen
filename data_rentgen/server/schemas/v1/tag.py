# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import BaseModel, ConfigDict, Field

from data_rentgen.server.schemas.v1.pagination import PaginateQueryV1


class TagValueResponseV1(BaseModel):
    id: int = Field(description="Tag value id")
    value: str = Field(description="Tag value")

    model_config = ConfigDict(from_attributes=True)


class TagResponseV1(BaseModel):
    id: int = Field(description="Tag id")
    name: str = Field(description="Tag name")
    values: list[TagValueResponseV1] = Field(default_factory=list, description="Values for the tag")

    model_config = ConfigDict(from_attributes=True)


class TagDetailedResponseV1(BaseModel):
    id: int = Field(description="Tag id")
    data: TagResponseV1 = Field(description="Tag data")

    model_config = ConfigDict(from_attributes=True)


class TagPaginateQueryV1(PaginateQueryV1):
    """Query params for Tag paginate request."""

    tag_id: list[int] = Field(
        default_factory=list,
        description="Ids of tags to fetch specific items only",
        examples=[[123]],
    )
    search_query: str | None = Field(
        default=None,
        min_length=3,
        description="Search query, partial match by tag name or any value",
        examples=["my tag"],
    )

    model_config = ConfigDict(extra="forbid")
