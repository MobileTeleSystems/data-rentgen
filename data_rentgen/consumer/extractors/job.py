# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from urllib.parse import urlparse

from data_rentgen.consumer.openlineage.job import OpenLineageJob
from data_rentgen.consumer.openlineage.run_facets import OpenLineageParentJob
from data_rentgen.dto import JobDTO, JobTypeDTO, LocationDTO


def extract_parent_job(job: OpenLineageParentJob) -> JobDTO:
    return JobDTO(
        name=job.name,
        location=extract_job_location(job),
    )


def extract_job(job: OpenLineageJob) -> JobDTO:
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


def extract_job_type(job: OpenLineageJob) -> JobTypeDTO | None:
    if job.facets.jobType:
        integration_type = job.facets.jobType.integration
        job_type = job.facets.jobType.jobType
        type_ = f"{integration_type}_{job_type}" if job_type else integration_type
        return JobTypeDTO(type=type_.upper())

    return None
