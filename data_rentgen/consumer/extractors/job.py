# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from urllib.parse import urlparse

from data_rentgen.consumer.openlineage.job import OpenLineageJob
from data_rentgen.consumer.openlineage.run_facets import OpenLineageParentJob
from data_rentgen.dto import JobDTO, LocationDTO


def extract_job(job: OpenLineageJob | OpenLineageParentJob) -> JobDTO:
    return JobDTO(name=job.name, location=extract_job_location(job))


def extract_job_location(job: OpenLineageJob | OpenLineageParentJob) -> LocationDTO:
    url = urlparse(job.namespace)
    scheme = url.scheme or "unknown"
    netloc = url.netloc or url.path
    return LocationDTO(
        type=scheme,
        name=netloc,
        addresses=[f"{scheme}://{netloc}"],
    )
