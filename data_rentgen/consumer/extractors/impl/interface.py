# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Protocol

from data_rentgen.consumer.openlineage.dataset import (
    OpenLineageDataset,
    OpenLineageInputDataset,
    OpenLineageOutputDataset,
)
from data_rentgen.consumer.openlineage.run_event import OpenLineageRunEvent
from data_rentgen.dto import ColumnLineageDTO, DatasetSymlinkDTO, InputDTO, OperationDTO, OutputDTO, RunDTO


class ExtractorInterface(Protocol):
    """All Extractors should implement this interface"""

    def match(self, event: OpenLineageRunEvent) -> bool:
        """
        Returns true if this extractor should be used to process the event.

        This is not a part of GenericExtractor implementation to avoid
        accidentally implementing extractor which accepts every event.
        """
        ...

    def is_operation(self, event: OpenLineageRunEvent) -> bool:
        """
        Returns true of this event should be used to extract OperationDTO.
        Returns false if this event should be used to extract RunDTO.

        This is not a part of GenericExtractor implementation
        to make developers aware that this logic is implementation-specific.
        """
        ...

    def extract_run(self, event: OpenLineageRunEvent) -> RunDTO: ...

    def extract_operation(self, event: OpenLineageRunEvent) -> OperationDTO: ...

    def extract_input(
        self,
        operation: OperationDTO,
        dataset: OpenLineageInputDataset,
    ) -> tuple[InputDTO, list[DatasetSymlinkDTO]]: ...

    def extract_output(
        self,
        operation: OperationDTO,
        dataset: OpenLineageOutputDataset,
    ) -> tuple[OutputDTO, list[DatasetSymlinkDTO]]: ...

    def extract_column_lineage(
        self,
        operation: OperationDTO,
        output_dataset: OpenLineageDataset,
    ) -> list[ColumnLineageDTO]: ...
