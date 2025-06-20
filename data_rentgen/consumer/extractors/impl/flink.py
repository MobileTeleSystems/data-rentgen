# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from data_rentgen.consumer.extractors.generic import GenericExtractor
from data_rentgen.consumer.openlineage.dataset import OpenLineageDataset
from data_rentgen.consumer.openlineage.dataset_facets import (
    OpenLineageSymlinkIdentifier,
    OpenLineageSymlinkType,
)
from data_rentgen.consumer.openlineage.run_event import OpenLineageRunEvent
from data_rentgen.dto import DatasetDTO, DatasetSymlinkDTO, RunDTO


class FlinkExtractor(GenericExtractor):
    def match(self, event: OpenLineageRunEvent) -> bool:
        return bool(event.job.facets.jobType and event.job.facets.jobType.integration == "FLINK")

    def is_operation(self, event: OpenLineageRunEvent) -> bool:
        return event.job.facets.jobType.jobType == "JOB"  # type: ignore[union-attr]

    def extract_run(self, event: OpenLineageRunEvent) -> RunDTO:
        run = super().extract_run(event)
        self._enrich_run_identifiers(run, event)
        self._enrich_run_log_url(run, event)
        return run

    def _enrich_run_identifiers(self, run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
        # https://github.com/OpenLineage/OpenLineage/pull/3744
        flink_job_facet = event.run.facets.flink_job
        if flink_job_facet:
            run.external_id = flink_job_facet.jobId
        return run

    def _enrich_run_log_url(self, run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
        # https://github.com/OpenLineage/OpenLineage/pull/3744
        flink_job_facet = event.run.facets.flink_job
        if not flink_job_facet:
            return run

        flink_base_url = event.job.namespace.rstrip("/")
        if not flink_base_url.startswith("http"):
            return run

        run.running_log_url = f"{flink_base_url}/#/job/running/{flink_job_facet.jobId}"
        run.persistent_log_url = f"{flink_base_url}/#/job/completed/{flink_job_facet.jobId}"
        return run

    def _extract_dataset_and_symlinks(
        self,
        dataset: OpenLineageDataset,
        symlink_identifiers: list[OpenLineageSymlinkIdentifier],
    ) -> tuple[DatasetDTO, list[DatasetSymlinkDTO]]:
        # Exclude Kafka fake symlinks produced by Flink 2.x integration.
        # See https://github.com/OpenLineage/OpenLineage/pull/3657
        symlink_identifiers = [
            identifier
            for identifier in symlink_identifiers
            if not (identifier.namespace.startswith("kafka://") and identifier.type == OpenLineageSymlinkType.TABLE)
        ]
        return super()._extract_dataset_and_symlinks(dataset, symlink_identifiers)
