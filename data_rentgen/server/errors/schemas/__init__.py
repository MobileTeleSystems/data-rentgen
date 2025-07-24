# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from data_rentgen.server.errors.schemas.already_exists import AlreadyExistsSchema
from data_rentgen.server.errors.schemas.invalid_request import InvalidRequestSchema
from data_rentgen.server.errors.schemas.logout import LogoutErrorSchema
from data_rentgen.server.errors.schemas.not_allowed import NotAllowedSchema
from data_rentgen.server.errors.schemas.not_authorized import (
    NotAuthorizedRedirectSchema,
    NotAuthorizedSchema,
)
from data_rentgen.server.errors.schemas.not_found import NotFoundSchema
from data_rentgen.server.errors.schemas.not_implemented import NotImplementedErrorSchema

__all__ = [
    "AlreadyExistsSchema",
    "InvalidRequestSchema",
    "LogoutErrorSchema",
    "NotAllowedSchema",
    "NotAuthorizedRedirectSchema",
    "NotAuthorizedSchema",
    "NotFoundSchema",
    "NotImplementedErrorSchema",
]
