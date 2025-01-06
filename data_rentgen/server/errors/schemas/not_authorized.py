# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import http
from typing import Any, Literal

from data_rentgen.exceptions.auth import AuthorizationError
from data_rentgen.exceptions.redirect import RedirectError
from data_rentgen.server.errors.base import BaseErrorSchema
from data_rentgen.server.errors.registration import register_error_response


@register_error_response(
    exception=AuthorizationError,
    status=http.HTTPStatus.UNAUTHORIZED,
)
class NotAuthorizedSchema(BaseErrorSchema):
    code: Literal["unauthorized"] = "unauthorized"
    details: Any = None


@register_error_response(
    exception=RedirectError,
    status=http.HTTPStatus.UNAUTHORIZED,
)
class NotAuthorizedRedirectSchema(BaseErrorSchema):
    """
    The reason of using UNAUTHORIZED instead of strict redirect is:
    Fronted is using `fetch()` function which can't handle redirect responses
    https://github.com/whatwg/fetch/issues/601
    """

    code: Literal["auth_redirect"] = "auth_redirect"
