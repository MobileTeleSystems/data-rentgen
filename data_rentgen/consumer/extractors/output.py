# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.openlineage.dataset import OpenLineageOutputDataset
from data_rentgen.dto import OutputDTO, OutputTypeDTO


def extract_output(dataset: OpenLineageOutputDataset) -> OutputDTO:
    lifecycle_change = dataset.facets.lifecycleStateChange
    if lifecycle_change:
        output_type = OutputTypeDTO(lifecycle_change.lifecycleStateChange)
    else:
        output_type = OutputTypeDTO.APPEND

    result = OutputDTO(type=output_type)
    if dataset.outputFacets.outputStatistics:
        result.num_rows = dataset.outputFacets.outputStatistics.rows
        result.num_bytes = dataset.outputFacets.outputStatistics.bytes
        result.num_files = dataset.outputFacets.outputStatistics.files

    return result
