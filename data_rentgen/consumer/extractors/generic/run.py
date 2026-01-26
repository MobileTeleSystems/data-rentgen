# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from abc import ABC, abstractmethod

from data_rentgen.dto import (
    JobDTO,
    RunDTO,
    RunStatusDTO,
    TagDTO,
    TagValueDTO,
)
from data_rentgen.openlineage.job import OpenLineageJob
from data_rentgen.openlineage.run_event import (
    OpenLineageRunEvent,
    OpenLineageRunEventType,
)
from data_rentgen.openlineage.run_facets import (
    OpenLineageParentJob,
    OpenLineageParentRunFacet,
)


class RunExtractorMixin(ABC):
    @abstractmethod
    def extract_job(self, job: OpenLineageJob) -> JobDTO:
        pass

    @abstractmethod
    def extract_parent_job(self, job: OpenLineageJob | OpenLineageParentJob) -> JobDTO:
        pass

    def extract_run(self, event: OpenLineageRunEvent) -> RunDTO:
        """
        Extract RunDTO from specific event
        """
        run = RunDTO(
            id=event.run.runId,  # type: ignore [arg-type]
            job=self.extract_job(event.job),
            parent_run=self.extract_parent_run(event.run.facets.parent) if event.run.facets.parent else None,
        )
        self._enrich_run_status(run, event)
        self._enrich_run_tags(run, event)
        return run

    def extract_parent_run(self, facet: OpenLineageParentRunFacet | OpenLineageRunEvent) -> RunDTO:
        """
        Extract RunDTO from parent run reference
        """
        return RunDTO(
            id=facet.run.runId,
            job=self.extract_parent_job(facet.job),
        )

    def _enrich_run_status(self, run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
        match event.eventType:
            case OpenLineageRunEventType.START:
                run.started_at = event.eventTime
                run.status = RunStatusDTO.STARTED
            case OpenLineageRunEventType.RUNNING:
                run.status = RunStatusDTO.STARTED
            case OpenLineageRunEventType.COMPLETE:
                run.ended_at = event.eventTime
                run.status = RunStatusDTO.SUCCEEDED
            case OpenLineageRunEventType.FAIL:
                run.ended_at = event.eventTime
                run.status = RunStatusDTO.FAILED
            case OpenLineageRunEventType.ABORT:
                run.ended_at = event.eventTime
                run.status = RunStatusDTO.KILLED
            case OpenLineageRunEventType.OTHER:
                # OTHER is used only to update run statistics
                pass
        return run

    def _enrich_run_tags(self, run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
        if event.run.facets.processing_engine:
            client_tag_value = TagValueDTO(
                tag=TagDTO(name=f"{event.run.facets.processing_engine.name.lower()}.version"),
                value=str(event.run.facets.processing_engine.version),
            )
            adapter_tag_value = TagValueDTO(
                tag=TagDTO(name="openlineage_adapter.version"),
                value=str(event.run.facets.processing_engine.openlineageAdapterVersion),
            )
            # we don't store run tags, everything is merged into job tags
            run.job.tag_values.add(client_tag_value)
            run.job.tag_values.add(adapter_tag_value)

        if not event.run.facets.tags:
            return run

        for raw_tag in event.run.facets.tags.tags:
            key = raw_tag.key
            if key == "openlineage_client_version":
                # https://github.com/OpenLineage/OpenLineage/blob/1.42.1/client/python/src/openlineage/client/client.py#L460
                tag_value = TagValueDTO(
                    tag=TagDTO(name="openlineage_client.version"),
                    value=raw_tag.value,
                )
                run.job.tag_values.add(tag_value)
        return run
