# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Generic, TypeVar

from pydantic import BaseModel


class BaseErrorSchema(BaseModel):
    code: str
    message: str
    details: Any


ErrorModel = TypeVar("ErrorModel", bound=BaseErrorSchema)


class APIErrorSchema(BaseModel, Generic[ErrorModel]):
    error: ErrorModel
