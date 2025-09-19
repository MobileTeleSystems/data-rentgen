# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class UserDTO:
    name: str
    id: int | None = field(default=None, compare=False)

    @property
    def unique_key(self) -> tuple:
        return (self.name.lower(),)

    def merge(self, new: UserDTO) -> UserDTO:
        self.id = new.id or self.id
        return self
