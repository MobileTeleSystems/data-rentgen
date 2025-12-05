# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import http
from typing import Any, Literal

from data_rentgen.server.errors.base import BaseErrorSchema
from data_rentgen.server.errors.registration import register_error_response


@register_error_response(
    exception=NotImplementedError,
    status=http.HTTPStatus.NOT_IMPLEMENTED,
)
class NotImplementedErrorSchema(BaseErrorSchema):
    code: Literal["not_implemented"] = "not_implemented"
    details: Any = None
