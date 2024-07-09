# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass

from data_rentgen.dto.location import LocationDTO


@dataclass
class JobDTO:
    name: str
    location: LocationDTO
