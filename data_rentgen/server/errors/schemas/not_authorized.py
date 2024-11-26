# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import http
from typing import Any, Literal

from data_rentgen.exceptions.auth import AuthorizationError
from data_rentgen.server.errors.base import BaseErrorSchema
from data_rentgen.server.errors.registration import register_error_response


@register_error_response(
    exception=AuthorizationError,
    status=http.HTTPStatus.UNAUTHORIZED,
)
class NotAuthorizedSchema(BaseErrorSchema):
    code: Literal["unauthorized"] = "unauthorized"
    details: Any = None
