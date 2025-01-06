# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from typing import Any


class ApplicationError(ABC, Exception):
    """Base class for all exceptions raised by data_rentgen."""

    @property
    @abstractmethod
    def message(self) -> str:
        """Message string"""
        ...

    @property
    @abstractmethod
    def details(self) -> Any:
        """Details related to specific error"""
        ...

    def __str__(self) -> str:
        return self.message
