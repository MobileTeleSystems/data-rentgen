# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.openlineage.dataset import (
    OpenLineageDataset,
    OpenLineageInputDataset,
    OpenLineageOutputDataset,
)
from data_rentgen.dto import InteractionDTO, InteractionTypeDTO
from data_rentgen.dto.schema import SchemaDTO


def extract_input_interaction(input_dataset: OpenLineageInputDataset) -> InteractionDTO:
    interaction = InteractionDTO(type=InteractionTypeDTO.READ)
    enrich_interaction_statistics(interaction, input_dataset)
    return interaction


def extract_output_interaction(output_dataset: OpenLineageOutputDataset) -> InteractionDTO:
    lifecycle_change = output_dataset.facets.lifecycleStateChange
    if lifecycle_change:
        interaction_type = InteractionTypeDTO(lifecycle_change.lifecycleStateChange)
    else:
        interaction_type = InteractionTypeDTO.APPEND

    interaction = InteractionDTO(type=interaction_type)
    enrich_interaction_statistics(interaction, output_dataset)
    return interaction


def extract_interaction_schema(dataset: OpenLineageDataset) -> SchemaDTO | None:
    if not dataset.facets.datasetSchema:
        return None

    fields = dataset.facets.datasetSchema.fields
    return SchemaDTO(
        fields=[field.model_dump(exclude_unset=True, exclude_none=True) for field in fields],
    )


def enrich_interaction_statistics(
    interaction: InteractionDTO,
    dataset: OpenLineageInputDataset | OpenLineageOutputDataset,
) -> InteractionDTO:
    if isinstance(dataset, OpenLineageInputDataset) and dataset.inputFacets.dataQualityMetrics:
        interaction.num_rows = dataset.inputFacets.dataQualityMetrics.rows
        interaction.num_bytes = dataset.inputFacets.dataQualityMetrics.bytes
        interaction.num_files = dataset.inputFacets.dataQualityMetrics.files

    elif isinstance(dataset, OpenLineageOutputDataset) and dataset.outputFacets.outputStatistics:
        interaction.num_rows = dataset.outputFacets.outputStatistics.rows
        interaction.num_bytes = dataset.outputFacets.outputStatistics.bytes
        interaction.num_files = dataset.outputFacets.outputStatistics.files

    return interaction
