# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import date
from enum import Enum
from uuid import UUID

from fastapi import Query
from pydantic import BaseModel, ConfigDict, Field, FutureDate

from data_rentgen.server.schemas.v1.pagination import PaginateQueryV1


class PersonalTokenPaginateQueryV1(PaginateQueryV1):
    personal_token_id: list[UUID] = Field(Query(default_factory=list), description="Token id")

    model_config = ConfigDict(extra="forbid")


class PersonalTokenCreateRequestV1(BaseModel):
    name: str = Field(description="Token name")
    until: FutureDate | None = Field(default=None, description="Token expiration date")

    model_config = ConfigDict(extra="forbid")


class PersonalTokenResetRequestV1(BaseModel):
    until: FutureDate | None = Field(default=None, description="Token expiration date")

    model_config = ConfigDict(extra="forbid")


class PersonalTokenScopeV1(str, Enum):
    ALL_READ = "all:read"
    ALL_WRITE = "all:write"

    def __str__(self) -> str:
        return self.value


class PersonalTokenResponseV1(BaseModel):
    id: UUID = Field(description="Token id")
    name: str = Field(description="Token name")
    scopes: list[PersonalTokenScopeV1] = Field(description="Token scopes")
    since: date = Field(description="Token creation date")
    until: date = Field(description="Token expiration date")

    model_config = ConfigDict(from_attributes=True)


class PersonalTokenDetailedResponseV1(BaseModel):
    id: UUID = Field(description="Personal token id")
    data: PersonalTokenResponseV1 = Field(description="Personal token data")

    model_config = ConfigDict(from_attributes=True)


class PersonalTokenCreatedDetailedResponseV1(PersonalTokenDetailedResponseV1):
    content: str = Field(description="Personal token string")
