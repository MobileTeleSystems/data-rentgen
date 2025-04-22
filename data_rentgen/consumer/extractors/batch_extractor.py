# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from data_rentgen.consumer.extractors.batch_extraction_result import BatchExtractionResult
from data_rentgen.consumer.extractors.column_lineage import extract_column_lineage
from data_rentgen.consumer.extractors.input import extract_input
from data_rentgen.consumer.extractors.operation import extract_operation
from data_rentgen.consumer.extractors.output import extract_output
from data_rentgen.consumer.extractors.run import extract_run
from data_rentgen.consumer.openlineage.job_facets.job_type import OpenLineageJobType
from data_rentgen.consumer.openlineage.run_event import OpenLineageRunEvent
from data_rentgen.dto import (
    DatasetDTO,
)


class BatchExtractor:
    def __init__(self) -> None:
        self.dataset_cache: dict[tuple[str, str], DatasetDTO] = {}
        self.result = BatchExtractionResult()

    def add_events(self, events: list[OpenLineageRunEvent]) -> BatchExtractionResult:
        for event in events:
            if event.job.facets.jobType and event.job.facets.jobType.jobType == OpenLineageJobType.JOB:
                self.extract_operation(event)
            else:
                self.extract_run(event)

        return self.result

    def extract_run(self, event: OpenLineageRunEvent) -> None:
        run = extract_run(event)
        self.result.add_run(run)

    def extract_operation(self, event: OpenLineageRunEvent) -> None:
        operation = extract_operation(event)
        self.result.add_operation(operation)

        for input_dataset in event.inputs:
            input_dto, symlink_dtos = extract_input(operation, input_dataset)

            self.result.add_input(input_dto)
            dataset_dto_cache_key = (input_dataset.namespace, input_dataset.name)
            self.dataset_cache[dataset_dto_cache_key] = self.result.get_dataset(input_dto.dataset.unique_key)

            for symlink_dto in symlink_dtos:
                self.result.add_dataset_symlink(symlink_dto)

        for output_dataset in event.outputs:
            output_dto, symlink_dtos = extract_output(operation, output_dataset)

            self.result.add_output(output_dto)
            dataset_dto_cache_key = (output_dataset.namespace, output_dataset.name)
            self.dataset_cache[dataset_dto_cache_key] = self.result.get_dataset(output_dto.dataset.unique_key)

            for symlink_dto in symlink_dtos:
                self.result.add_dataset_symlink(symlink_dto)

        for dataset in event.inputs + event.outputs:
            column_lineage = extract_column_lineage(operation, dataset, self.dataset_cache)
            for item in column_lineage:
                self.result.add_column_lineage(item)
