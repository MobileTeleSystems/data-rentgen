# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.extractors.dataset import extract_io_dataset
from data_rentgen.consumer.extractors.schema import extract_schema
from data_rentgen.consumer.openlineage.dataset import OpenLineageOutputDataset
from data_rentgen.dto import OutputDTO, OutputTypeDTO
from data_rentgen.dto.operation import OperationDTO


def extract_output(
    operation: OperationDTO,
    dataset: OpenLineageOutputDataset,
) -> OutputDTO:
    lifecycle_change = dataset.facets.lifecycleStateChange
    if lifecycle_change:
        output_type = OutputTypeDTO(lifecycle_change.lifecycleStateChange)
    else:
        output_type = OutputTypeDTO.APPEND

    result = OutputDTO(
        type=output_type,
        operation=operation,
        dataset=extract_io_dataset(dataset),
        schema=extract_schema(dataset),
    )
    if dataset.outputFacets.outputStatistics:
        result.num_rows = dataset.outputFacets.outputStatistics.rows
        result.num_bytes = dataset.outputFacets.outputStatistics.bytes
        result.num_files = dataset.outputFacets.outputStatistics.files

    return result
