# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from enum import Enum

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
    def _missing_(cls, value):
        if value in {"SQL_JOB", "RDD_JOB"}:
            return cls.JOB
        return None


class OpenLineageJobProcessingType(str, Enum):
    """Job processing type.
    See [JobTypeJobFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/JobTypeJobFacet.json).
    """

    BATCH = "BATCH"
    STREAMING = "STREAMING"
    NONE = "NONE"


class OpenLineageJobTypeJobFacet(OpenLineageJobFacet):
    """Job facet describing job type.
    See [JobTypeJobFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/JobTypeJobFacet.json).
    """

    integration: OpenLineageJobIntegrationType
    jobType: OpenLineageJobType
    processingType: OpenLineageJobProcessingType = OpenLineageJobProcessingType.NONE
