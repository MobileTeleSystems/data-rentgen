# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.extractors.dataset import extract_dataset
from data_rentgen.consumer.extractors.schema import extract_schema
from data_rentgen.consumer.openlineage.dataset import OpenLineageInputDataset
from data_rentgen.dto import InputDTO
from data_rentgen.dto.operation import OperationDTO


def extract_input(
    operation: OperationDTO,
    dataset: OpenLineageInputDataset,
) -> InputDTO:
    result = InputDTO(
        operation=operation,
        dataset=extract_dataset(dataset),
        schema=extract_schema(dataset),
    )
    if dataset.inputFacets.dataQualityMetrics:
        result.num_rows = dataset.inputFacets.dataQualityMetrics.rows
        result.num_bytes = dataset.inputFacets.dataQualityMetrics.bytes
        result.num_files = dataset.inputFacets.dataQualityMetrics.files
    return result
