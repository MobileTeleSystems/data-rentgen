# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from abc import ABC, abstractmethod

from data_rentgen.dto import (
    JobDTO,
    RunDTO,
    RunStatusDTO,
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
