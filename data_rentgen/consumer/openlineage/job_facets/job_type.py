# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from enum import Enum

from pydantic import field_validator

from data_rentgen.consumer.openlineage.job_facets.base import OpenLineageJobFacet


class OpenLineageJobIntegrationType(str, Enum):
    """Integration where job is running.
    See [JobTypeJobFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/JobTypeJobFacet.json).
    """

    SPARK = "SPARK"
    AIRFLOW = "AIRFLOW"

    def __str__(self) -> str:
        return self.value


class OpenLineageJobType(str, Enum):
    """Job type.
    See [JobTypeJobFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/JobTypeJobFacet.json).
    """

    APPLICATION = "APPLICATION"
    JOB = "JOB"
    DAG = "DAG"
    TASK = "TASK"

    def __str__(self) -> str:
        return self.value

    @classmethod
    def _missing_(cls, value):  # noqa: WPS120
        if value in {"SQL_JOB", "RDD_JOB"}:
            return cls.JOB
        return None


class OpenLineageJobProcessingType(str, Enum):
    """Job processing type.
    See [JobTypeJobFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/JobTypeJobFacet.json).
    """

    BATCH = "BATCH"
    STREAMING = "STREAMING"


class OpenLineageJobTypeJobFacet(OpenLineageJobFacet):
    """Job facet describing job type.
    See [JobTypeJobFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/JobTypeJobFacet.json).
    """

    integration: OpenLineageJobIntegrationType
    jobType: OpenLineageJobType
    processingType: OpenLineageJobProcessingType | None = None

    @field_validator("processingType", mode="before")
    @classmethod
    def _validate_processing_type(cls, processing_type: str):
        if processing_type == "NONE":
            return None
        return processing_type
