# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import http
from typing import Any, Literal

from data_rentgen.exceptions.auth import LogoutError
from data_rentgen.server.errors.base import BaseErrorSchema
from data_rentgen.server.errors.registration import register_error_response


@register_error_response(
    exception=LogoutError,
    status=http.HTTPStatus.BAD_REQUEST,
)
class LogoutErrorSchema(BaseErrorSchema):
    code: Literal["logout_error"] = "logout_error"
    details: Any = None
