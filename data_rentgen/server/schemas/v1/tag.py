# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import BaseModel, ConfigDict, Field, model_validator

from data_rentgen.server.schemas.v1.pagination import PaginateQueryV1


class NestedTagValueResponseV1(BaseModel):
    id: int = Field(description="Tag value id")
    value: str = Field(description="Tag value")

    model_config = ConfigDict(from_attributes=True)


class TagWithValuesResponseV1(BaseModel):
    id: int = Field(description="Tag id")
    name: str = Field(description="Tag name")
    values: list[NestedTagValueResponseV1] = Field(default_factory=list, description="Values for the tag")

    model_config = ConfigDict(from_attributes=True)


class TagResponseV1(BaseModel):
    id: int = Field(description="Tag id")
    name: str = Field(description="Tag name")

    model_config = ConfigDict(from_attributes=True)


class TagValueResponseV1(BaseModel):
    id: int = Field(description="TagValue id")
    tag_id: int = Field(description="Tag id")
    value: str = Field(description="Tag value")

    model_config = ConfigDict(from_attributes=True)


class TagDetailedResponseV1(BaseModel):
    id: int = Field(description="Tag id")
    data: TagResponseV1 = Field(description="Tag data")

    model_config = ConfigDict(from_attributes=True)


class TagValueDetailedResponseV1(BaseModel):
    id: int = Field(description="TagValue id")
    data: TagValueResponseV1 = Field(description="TagValue data")

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
        description="Search query, partial match by tag name",
        examples=["my.tag"],
    )

    model_config = ConfigDict(extra="forbid")


class TagValuePaginateQueryV1(PaginateQueryV1):
    """Query params for TagValue paginate request."""

    tag_id: int | None = Field(
        default=None,
        description="Get only values for specific tag",
        examples=[[123]],
    )
    tag_value_id: list[int] = Field(
        default_factory=list,
        description="Ids of tag_values to fetch specific items only",
        examples=[[123]],
    )
    search_query: str | None = Field(
        default=None,
        min_length=3,
        description="Search query, partial match by tag value",
        examples=["my value"],
    )

    @model_validator(mode="after")
    def _check_tag_id_and_tag_value_id(self):
        if not self.tag_id and not self.tag_value_id:
            msg = "input should contain either 'tag_id' or 'tag_value_id' field"
            raise ValueError(msg)
        return self

    model_config = ConfigDict(extra="forbid")
