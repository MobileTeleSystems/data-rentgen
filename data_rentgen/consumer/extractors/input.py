# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.openlineage.dataset import OpenLineageInputDataset
from data_rentgen.dto import InputDTO


def extract_input(dataset: OpenLineageInputDataset) -> InputDTO:
    result = InputDTO()
    if dataset.inputFacets.dataQualityMetrics:
        result.num_rows = dataset.inputFacets.dataQualityMetrics.rows
        result.num_bytes = dataset.inputFacets.dataQualityMetrics.bytes
        result.num_files = dataset.inputFacets.dataQualityMetrics.files
    return result
