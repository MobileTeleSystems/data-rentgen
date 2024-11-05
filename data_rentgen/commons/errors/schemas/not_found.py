# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import http
from typing import Any

from pydantic import BaseModel
from typing_extensions import Literal

from data_rentgen.commons.errors.base import BaseErrorSchema
from data_rentgen.commons.errors.registration import register_error_response
from data_rentgen.commons.exceptions.entity import EntityNotFoundError


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

    def to_exception(self) -> EntityNotFoundError:
        return EntityNotFoundError(
            entity_type=self.details.entity_type,
            field=self.details.field,
            value=self.details.value,
        )
