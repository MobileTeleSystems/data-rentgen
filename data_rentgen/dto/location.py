# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass


@dataclass
class LocationDTO:
    type: str
    name: str
    addresses: list[str]
