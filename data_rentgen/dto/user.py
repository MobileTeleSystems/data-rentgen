# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class UserDTO:
    name: str
    id: int | None = field(default=None, compare=False)

    @property
    def unique_key(self) -> tuple:
        return (self.name,)

    def merge(self, new: UserDTO) -> UserDTO:
        return UserDTO(
            name=self.name,
            id=new.id or self.id,
        )
