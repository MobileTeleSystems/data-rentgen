# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass


@dataclass(slots=True)
class LocationDTO:
    type: str
    name: str
    addresses: list[str]

    @property
    def full_name(self) -> str:
        return f"{self.type}://{self.name}"
