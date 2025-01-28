# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Any

from data_rentgen.exceptions.base import ApplicationError


class RedirectError(ApplicationError):
    """Error which contains redirect url for authorization."""

    def __init__(self, message: str, details: Any = None) -> None:
        self._message = message
        self._details = details

    @property
    def message(self) -> str:
        return self._message

    @property
    def details(self) -> Any:
        return self._details
