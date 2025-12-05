# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from data_rentgen.dto.pagination import PaginationDTO

T = TypeVar("T", bound=BaseModel)


class PageMetaResponseV1(BaseModel):
    """Page metadata response."""

    page: int = Field(description="Page number")
    page_size: int = Field(description="Number of items per page")
    total_count: int = Field(description="Total number of items")
    pages_count: int = Field(description="Number of items returned in current page")
    has_next: bool = Field(description="Is there a next page")
    has_previous: bool = Field(description="Is there a next page")
    next_page: int | None = Field(description="Next page number, if any")
    previous_page: int | None = Field(description="Previous page number, if any")


class PaginateQueryV1(BaseModel):
    """Basic class with pagination query params."""

    page: int = Field(gt=0, default=1, description="Page number")
    page_size: int = Field(gt=0, le=50, default=20, description="Number of items per page")

    model_config = ConfigDict(extra="forbid")


class PageResponseV1(BaseModel, Generic[T]):
    """Page response."""

    meta: PageMetaResponseV1 = Field(description="Page metadata")
    items: list[T] = Field(description="Page content")

    @classmethod
    def from_pagination(cls, pagination: PaginationDTO):
        return cls(
            meta=PageMetaResponseV1(
                page=pagination.page,
                pages_count=pagination.pages_count,
                page_size=pagination.page_size,
                total_count=pagination.total_count,
                has_next=pagination.has_next,
                has_previous=pagination.has_previous,
                next_page=pagination.next_page,
                previous_page=pagination.previous_page,
            ),
            items=pagination.items,
        )
