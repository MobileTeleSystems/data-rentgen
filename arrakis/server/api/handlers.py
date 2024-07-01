# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import http
import logging

from asgi_correlation_id import correlation_id
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

from arrakis.server.errors.base import APIErrorSchema, BaseErrorSchema
from arrakis.server.errors.registration import get_response_for_exception
from arrakis.server.exceptions import ApplicationError
from arrakis.server.settings.server import ServerSettings

logger = logging.getLogger(__name__)


def http_exception_handler(_request: Request, exc: HTTPException) -> Response:
    content = BaseErrorSchema(
        code=http.HTTPStatus(exc.status_code).name.lower(),
        message=exc.detail,
        details=None,
    )
    return exception_json_response(
        status=exc.status_code,
        content=content,
        headers=exc.headers,
    )


def unknown_exception_handler(request: Request, exc: Exception) -> Response:
    logger.exception("Got unhandled error: %s", exc, exc_info=exc)

    server: ServerSettings = request.app.state.settings.server
    details = None
    if request.app.debug:
        details = exc.args

    content = BaseErrorSchema(
        code="unknown",
        message="Got unhandled exception. Please contact support",
        details=details,
    )
    return exception_json_response(
        status=http.HTTPStatus.INTERNAL_SERVER_ERROR.value,
        content=content,
        # https://github.com/snok/asgi-correlation-id#exception-handling
        headers={server.request_id.header_name: correlation_id.get() or ""},
    )


def validation_exception_handler(request: Request, exc: RequestValidationError) -> Response:
    response = get_response_for_exception(ValidationError)
    if not response:
        return unknown_exception_handler(request, exc)

    # code and message is set within class implementation
    errors = []
    for error in exc.errors():
        # pydantic Error classes are not serializable, drop it
        error.get("ctx", {}).pop("error", None)
        errors.append(error)

    content = response.schema(  # type: ignore[call-arg]
        details=errors,
    )
    return exception_json_response(
        status=response.status,
        content=content,
    )


def application_exception_handler(request: Request, exc: ApplicationError) -> Response:
    response = get_response_for_exception(type(exc))
    if not response:
        return unknown_exception_handler(request, exc)

    logger.error("%s", exc, exc_info=logger.isEnabledFor(logging.DEBUG))

    # code is set within class implementation
    content = response.schema(  # type: ignore[call-arg]
        message=exc.message,
        details=exc.details,
    )
    return exception_json_response(
        status=response.status,
        content=content,
    )


def exception_json_response(
    status: int,
    content: BaseErrorSchema,
    headers: dict[str, str] | None = None,
) -> Response:
    content_type = type(content)
    error_schema = APIErrorSchema[content_type]  # type: ignore[valid-type]
    return Response(
        status_code=status,
        content=error_schema(error=content).model_dump_json(by_alias=True),
        media_type="application/json",
        headers=headers,
    )


def apply_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(ApplicationError, application_exception_handler)  # type: ignore[arg-type]
    app.add_exception_handler(
        RequestValidationError,
        validation_exception_handler,  # type: ignore[arg-type]
    )
    app.add_exception_handler(HTTPException, http_exception_handler)  # type: ignore[arg-type]
    app.add_exception_handler(Exception, unknown_exception_handler)
