# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from arrakis.server.errors.base import BaseErrorSchema
from arrakis.server.errors.registration import (
    APIErrorResponse,
    get_error_responses,
    get_response_for_exception,
    get_response_for_status_code,
)

__all__ = [
    "APIErrorResponse",
    "BaseErrorSchema",
    "get_error_responses",
    "get_response_for_exception",
    "get_response_for_status_code",
]
