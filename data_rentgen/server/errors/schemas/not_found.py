# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import http
from typing import Any

from pydantic import BaseModel
from typing_extensions import Literal

from data_rentgen.exceptions.entity import EntityNotFoundError
from data_rentgen.server.errors.base import BaseErrorSchema
from data_rentgen.server.errors.registration import register_error_response


class NotFoundDetailsSchema(BaseModel):
    entity_type: str
    field: str
    value: Any


@register_error_response(
    exception=EntityNotFoundError,
    status=http.HTTPStatus.NOT_FOUND,
)
class NotFoundSchema(BaseErrorSchema):
    code: Literal["not_found"] = "not_found"
    details: NotFoundDetailsSchema
