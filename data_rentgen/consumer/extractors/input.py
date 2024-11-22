# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.extractors.dataset import extract_dataset_and_symlinks
from data_rentgen.consumer.extractors.schema import extract_schema
from data_rentgen.consumer.openlineage.dataset import OpenLineageInputDataset
from data_rentgen.dto import DatasetSymlinkDTO, InputDTO
from data_rentgen.dto.operation import OperationDTO


def extract_input(
    operation: OperationDTO,
    dataset: OpenLineageInputDataset,
) -> tuple[InputDTO, list[DatasetSymlinkDTO]]:
    dataset_dto, symlinks = extract_dataset_and_symlinks(dataset)
    result = InputDTO(
        operation=operation,
        dataset=dataset_dto,
        schema=extract_schema(dataset),
    )
    if dataset.inputFacets.inputStatistics:
        result.num_rows = dataset.inputFacets.inputStatistics.rows
        result.num_bytes = dataset.inputFacets.inputStatistics.bytes
        result.num_files = dataset.inputFacets.inputStatistics.files
    return result, symlinks
