# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import http
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from data_rentgen.server.errors.base import BaseErrorSchema
from data_rentgen.server.errors.registration import register_error_response


class InvalidRequestBaseErrorSchema(BaseModel):
    loc: list[str | int] = Field(alias="location")
    msg: str = Field(alias="message")
    type: str = Field(alias="code")
    ctx: dict = Field(default_factory=dict, alias="context")
    input: Any = Field(default=None)

    model_config = ConfigDict(populate_by_name=True)


@register_error_response(
    exception=ValidationError,
    status=http.HTTPStatus.UNPROCESSABLE_ENTITY,
)
class InvalidRequestSchema(BaseErrorSchema):
    code: Literal["invalid_request"] = "invalid_request"
    message: Literal["Invalid request"] = "Invalid request"
    details: list[InvalidRequestBaseErrorSchema]
