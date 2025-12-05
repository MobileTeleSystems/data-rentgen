# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import http
from typing import Any, Literal

from data_rentgen.exceptions.auth import ActionNotAllowedError
from data_rentgen.server.errors.base import BaseErrorSchema
from data_rentgen.server.errors.registration import register_error_response


@register_error_response(
    exception=ActionNotAllowedError,
    status=http.HTTPStatus.FORBIDDEN,
)
class NotAllowedSchema(BaseErrorSchema):
    code: Literal["forbidden"] = "forbidden"
    details: Any = None
