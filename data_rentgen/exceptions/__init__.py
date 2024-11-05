# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.exceptions.base import ApplicationError
from data_rentgen.exceptions.entity import EntityNotFoundError

__all__ = [
    "ApplicationError",
    "EntityNotFoundError",
]
