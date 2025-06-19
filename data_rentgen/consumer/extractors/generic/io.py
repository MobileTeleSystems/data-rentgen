# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from abc import ABC, abstractmethod

from data_rentgen.consumer.openlineage.dataset import (
    OpenLineageDataset,
    OpenLineageInputDataset,
    OpenLineageOutputDataset,
)
from data_rentgen.consumer.openlineage.dataset_facets import (
    OpenLineageSchemaField,
)
from data_rentgen.dto import (
    DatasetDTO,
    DatasetSymlinkDTO,
    DatasetSymlinkTypeDTO,
    InputDTO,
    OperationDTO,
    OutputDTO,
    OutputTypeDTO,
    SchemaDTO,
)

METASTORE = DatasetSymlinkTypeDTO.METASTORE
WAREHOUSE = DatasetSymlinkTypeDTO.WAREHOUSE


class IOExtractorMixin(ABC):
    @abstractmethod
    def extract_dataset_and_symlinks(self, dataset: OpenLineageDataset) -> tuple[DatasetDTO, list[DatasetSymlinkDTO]]:
        pass

    def extract_input(
        self,
        operation: OperationDTO,
        dataset: OpenLineageInputDataset,
    ) -> tuple[InputDTO, list[DatasetSymlinkDTO]]:
        """
        Extract InputDTO with optional symlinks
        """
        resolved_dataset_dto, symlinks = self.extract_dataset_and_symlinks(dataset)

        result = InputDTO(
            operation=operation,
            dataset=resolved_dataset_dto,
            schema=self.extract_schema(dataset),
        )
        if dataset.inputFacets.inputStatistics:
            result.num_rows = dataset.inputFacets.inputStatistics.rows
            result.num_bytes = dataset.inputFacets.inputStatistics.bytes
            result.num_files = dataset.inputFacets.inputStatistics.files
        return result, symlinks

    def extract_output(
        self,
        operation: OperationDTO,
        dataset: OpenLineageOutputDataset,
    ) -> tuple[OutputDTO, list[DatasetSymlinkDTO]]:
        """
        Extract OutputDTO with optional symlinks
        """
        resolved_dataset_dto, symlinks = self.extract_dataset_and_symlinks(dataset)

        result = OutputDTO(
            operation=operation,
            dataset=resolved_dataset_dto,
            type=self._extract_output_type(dataset),
            schema=self.extract_schema(dataset),
        )
        if dataset.outputFacets.outputStatistics:
            result.num_rows = dataset.outputFacets.outputStatistics.rows
            result.num_bytes = dataset.outputFacets.outputStatistics.bytes
            result.num_files = dataset.outputFacets.outputStatistics.files

        return result, symlinks

    def _extract_output_type(self, dataset: OpenLineageDataset) -> OutputTypeDTO:
        if dataset.facets.lifecycleStateChange:
            return OutputTypeDTO[dataset.facets.lifecycleStateChange.lifecycleStateChange]
        return OutputTypeDTO.APPEND

    def _schema_field_to_json(self, field: OpenLineageSchemaField):
        result: dict = {
            "name": field.name,
        }
        if field.type:
            result["type"] = field.type

        description_ = (field.description or "").strip()
        if description_:
            result["description"] = description_

        if field.fields:
            result["fields"] = [self._schema_field_to_json(f) for f in field.fields]
        return result

    def extract_schema(self, dataset: OpenLineageDataset) -> SchemaDTO | None:
        """
        Extract SchemaDTO from specific dataset
        """
        if not dataset.facets.datasetSchema:
            return None

        fields = dataset.facets.datasetSchema.fields
        if not fields:
            return None

        return SchemaDTO(fields=[self._schema_field_to_json(field) for field in fields])
