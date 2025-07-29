# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import http
from typing import Any, Literal

from pydantic import BaseModel

from data_rentgen.exceptions.entity import EntityAlreadyExistsError
from data_rentgen.server.errors.base import BaseErrorSchema
from data_rentgen.server.errors.registration import register_error_response


class AlreadyExistsDetailsSchema(BaseModel):
    entity_type: str
    field: str
    value: Any


@register_error_response(
    exception=EntityAlreadyExistsError,
    status=http.HTTPStatus.CONFLICT,
)
class AlreadyExistsSchema(BaseErrorSchema):
    code: Literal["conflict"] = "conflict"
    details: AlreadyExistsDetailsSchema
