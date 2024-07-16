# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass
from enum import Enum

from data_rentgen.dto.location import LocationDTO


class JobTypeDTO(str, Enum):
    AIRFLOW_DAG = "AIRFLOW_DAG"
    AIRFLOW_TASK = "AIRFLOW_TASK"
    SPARK_APPLICATION = "SPARK_APPLICATION"

    def __str__(self) -> str:
        return self.value


@dataclass(slots=True)
class JobDTO:
    name: str
    location: LocationDTO
    type: JobTypeDTO | None = None
