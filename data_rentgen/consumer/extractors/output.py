# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.extractors.dataset import extract_dataset_and_symlinks
from data_rentgen.consumer.extractors.schema import extract_schema
from data_rentgen.consumer.openlineage.dataset import OpenLineageOutputDataset
from data_rentgen.dto import DatasetSymlinkDTO, OutputDTO, OutputTypeDTO
from data_rentgen.dto.operation import OperationDTO


def extract_output(
    operation: OperationDTO,
    dataset: OpenLineageOutputDataset,
) -> tuple[OutputDTO, list[DatasetSymlinkDTO]]:
    lifecycle_change = dataset.facets.lifecycleStateChange
    if lifecycle_change:
        output_type = OutputTypeDTO(lifecycle_change.lifecycleStateChange)
    else:
        output_type = OutputTypeDTO.APPEND
    dataset_dto, symlinks = extract_dataset_and_symlinks(dataset)
    result = OutputDTO(
        type=output_type,
        operation=operation,
        dataset=dataset_dto,
        schema=extract_schema(dataset),
    )
    if dataset.outputFacets.outputStatistics:
        result.num_rows = dataset.outputFacets.outputStatistics.rows
        result.num_bytes = dataset.outputFacets.outputStatistics.bytes
        result.num_files = dataset.outputFacets.outputStatistics.files

    return result, symlinks
