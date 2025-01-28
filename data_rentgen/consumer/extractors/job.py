# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from urllib.parse import urlparse

from data_rentgen.consumer.openlineage.job import OpenLineageJob
from data_rentgen.consumer.openlineage.run_facets import OpenLineageParentJob
from data_rentgen.dto import JobDTO, JobTypeDTO, LocationDTO


def extract_job(job: OpenLineageJob | OpenLineageParentJob) -> JobDTO:
    return JobDTO(
        name=job.name,
        location=extract_job_location(job),
        type=extract_job_type(job),
    )


def extract_job_location(job: OpenLineageJob | OpenLineageParentJob) -> LocationDTO:
    url = urlparse(job.namespace)
    scheme = url.scheme or "unknown"
    netloc = url.netloc or url.path
    return LocationDTO(
        type=scheme,
        name=netloc,
        addresses={f"{scheme}://{netloc}"},
    )


def extract_job_type(job: OpenLineageJob | OpenLineageParentJob) -> JobTypeDTO | None:
    if isinstance(job, OpenLineageJob) and job.facets.jobType:
        job_type = job.facets.jobType.jobType
        integration_type = job.facets.jobType.integration
        return JobTypeDTO(f"{integration_type}_{job_type}")

    return None
