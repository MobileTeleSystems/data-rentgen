# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from data_rentgen.consumer.extractors.generic import GenericExtractor
from data_rentgen.dto import DatasetDTO, OperationDTO, OutputTypeDTO, RunDTO
from data_rentgen.openlineage.dataset import OpenLineageDataset, OpenLineageOutputDataset
from data_rentgen.openlineage.dataset_facets import (
    OpenLineageColumnLineageDatasetFacetFieldRef,
    OpenLineageSymlinkIdentifier,
)
from data_rentgen.openlineage.run_event import OpenLineageRunEvent


class DbtExtractor(GenericExtractor):
    def match(self, event: OpenLineageRunEvent) -> bool:
        return bool(event.job.facets.jobType and event.job.facets.jobType.integration == "DBT")

    def is_operation(self, event: OpenLineageRunEvent) -> bool:
        return event.job.facets.jobType.jobType != "JOB"  # type: ignore[union-attr]

    def extract_run(self, event: OpenLineageRunEvent) -> RunDTO:
        run = super().extract_run(event)
        self._enrich_run_identifiers(run, event)
        return run

    def _enrich_run_identifiers(self, run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
        # https://github.com/OpenLineage/OpenLineage/pull/3738
        dbt_run_facet = event.run.facets.dbt_run
        if dbt_run_facet:
            run.external_id = dbt_run_facet.invocation_id
        return run

    def extract_operation(self, event: OpenLineageRunEvent) -> OperationDTO:
        # For Spark, DBT_MODEL --parent-> DBT_JOB = operation -> run,
        # and parent is always here.
        run = self.extract_parent_run(event.run.facets.parent)  # type: ignore[arg-type]
        operation = self._extract_operation(event, run)
        operation.group = event.job.facets.jobType.jobType  # type: ignore[union-attr]
        return operation

    def _extract_dataset_ref(
        self,
        dataset: OpenLineageDataset | OpenLineageColumnLineageDatasetFacetFieldRef | OpenLineageSymlinkIdentifier,
    ) -> DatasetDTO:
        dataset_dto = super()._extract_dataset_ref(dataset)
        # https://github.com/OpenLineage/OpenLineage/pull/3707
        dataset_dto.name = dataset.name.replace("None.", "")
        return dataset_dto

    def _extract_output_type(
        self,
        operation: OperationDTO,
        dataset: OpenLineageOutputDataset,
    ) -> OutputTypeDTO | None:
        # by default, model is not materialized, and is either VIEW or INSERT INTO
        result = super()._extract_output_type(operation, dataset)
        return result or OutputTypeDTO.APPEND
