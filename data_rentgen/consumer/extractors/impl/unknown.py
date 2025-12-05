# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from data_rentgen.consumer.extractors.generic import GenericExtractor
from data_rentgen.dto import OperationDTO, RunDTO
from data_rentgen.dto.run import RunStartReasonDTO
from data_rentgen.dto.user import UserDTO
from data_rentgen.openlineage.run_event import OpenLineageRunEvent


class UnknownExtractor(GenericExtractor):
    """
    Extractor for OpenLineage events produced by unknown integrations.
    Should always be last extractor in the chain, as it matches any event.
    """

    def match(self, event: OpenLineageRunEvent) -> bool:
        return True

    def extract_operation(self, event: OpenLineageRunEvent) -> OperationDTO:
        # if event has only dataRentgen_operation facet, it is Operation. Otherwise it is Run + Operation.
        if event.run.facets.parent and event.run.facets.dataRentgen_operation and not event.run.facets.dataRentgen_run:
            run = self.extract_parent_run(event.run.facets.parent)
        else:
            run = self.extract_run(event)

        operation = self._extract_operation(event, run)
        return self._enrich_operation_info(operation, event)

    def extract_run(self, event: OpenLineageRunEvent) -> RunDTO:
        run = super().extract_run(event)
        return self._enrich_run_info(run, event)

    def _enrich_run_info(self, run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
        run_info_facet = event.run.facets.dataRentgen_run
        if run_info_facet:
            run.external_id = run_info_facet.external_id
            run.attempt = run_info_facet.attempt
            run.persistent_log_url = run_info_facet.persistent_log_url
            run.running_log_url = run_info_facet.running_log_url
            run.start_reason = RunStartReasonDTO(run_info_facet.start_reason) if run_info_facet.start_reason else None
            run.user = UserDTO(name=run_info_facet.started_by_user) if run_info_facet.started_by_user else None
        return run

    def _enrich_operation_info(self, operation: OperationDTO, event: OpenLineageRunEvent):
        operation_info_facet = event.run.facets.dataRentgen_operation
        if operation_info_facet:
            operation.name = operation_info_facet.name or operation.name
            operation.description = operation_info_facet.description
            operation.position = operation_info_facet.position
            operation.group = operation_info_facet.group
        return operation

    def is_operation(self, event: OpenLineageRunEvent) -> bool:
        # This is heuristics which prevents creating useless operations
        return bool(event.run.facets.dataRentgen_operation or event.inputs or event.outputs)
