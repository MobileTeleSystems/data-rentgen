# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import logging
from collections import defaultdict

from data_rentgen.consumer.extractors.dataset import extract_dataset_ref
from data_rentgen.consumer.openlineage.dataset import OpenLineageDataset
from data_rentgen.consumer.openlineage.dataset_facets.column_lineage import (
    OpenLineageColumnLineageDatasetFacetFieldRef,
    OpenLineageColumnLineageDatasetFacetFieldTransformation,
)
from data_rentgen.dto import (
    ColumnLineageDTO,
    DatasetColumnRelationDTO,
    DatasetColumnRelationTypeDTO,
)
from data_rentgen.dto.dataset import DatasetDTO
from data_rentgen.dto.operation import OperationDTO

logger = logging.getLogger(__name__)

TRANSFORMATION_TYPE_DIRECT = "DIRECT"

TRANSFORMATION_SUBTYPE_MAP_MASKING = {
    "TRANSFORMATION": DatasetColumnRelationTypeDTO.TRANSFORMATION_MASKING,
    "AGGREGATION": DatasetColumnRelationTypeDTO.AGGREGATION_MASKING,
}

TRANSFORMATION_SUBTYPE_MAP = {
    "IDENTITY": DatasetColumnRelationTypeDTO.IDENTITY,
    "TRANSFORMATION": DatasetColumnRelationTypeDTO.TRANSFORMATION,
    "AGGREGATION": DatasetColumnRelationTypeDTO.AGGREGATION,
    "FILTER": DatasetColumnRelationTypeDTO.FILTER,
    "JOIN": DatasetColumnRelationTypeDTO.JOIN,
    "GROUP_BY": DatasetColumnRelationTypeDTO.GROUP_BY,
    "SORT": DatasetColumnRelationTypeDTO.SORT,
    "WINDOW": DatasetColumnRelationTypeDTO.WINDOW,
    "CONDITIONAL": DatasetColumnRelationTypeDTO.CONDITIONAL,
}


def extract_dataset_column_relation_type(
    transformation: OpenLineageColumnLineageDatasetFacetFieldTransformation,
) -> DatasetColumnRelationTypeDTO:
    result: DatasetColumnRelationTypeDTO | None = None
    if transformation.subtype:
        if transformation.masking:
            result = TRANSFORMATION_SUBTYPE_MAP_MASKING.get(transformation.subtype)
        else:
            result = TRANSFORMATION_SUBTYPE_MAP.get(transformation.subtype)

    return result or DatasetColumnRelationTypeDTO.UNKNOWN


def resolve_dataset_ref(
    dataset_ref: OpenLineageDataset | OpenLineageColumnLineageDatasetFacetFieldRef,
    dataset_dto_cache: dict[tuple[str, str], DatasetDTO],
):
    # extracting dataset for every column is expensive. cache it as much as we can
    dataset_cache_key = (dataset_ref.namespace, dataset_ref.name)
    if dataset_cache_key not in dataset_dto_cache:
        # https://github.com/OpenLineage/OpenLineage/issues/2938#issuecomment-2320377260
        dataset_dto_cache[dataset_cache_key] = extract_dataset_ref(dataset_ref)
    return dataset_dto_cache[dataset_cache_key]


def extract_column_lineage(
    operation: OperationDTO,
    target_dataset: OpenLineageDataset,
    dataset_cache: dict[tuple[str, str], DatasetDTO] | None = None,
) -> list[ColumnLineageDTO]:
    if not target_dataset.facets.columnLineage:
        return []

    dataset_cache = dataset_cache or {}
    target_dataset_dto = resolve_dataset_ref(target_dataset, dataset_cache)

    # Grouping column lineage by source+target dataset. This is unique combination within operation,
    # so we can use it to generate the same fingerprint for all dataset column relations
    datasets = {target_dataset_dto.unique_key: target_dataset_dto}
    dataset_column_relations: dict[tuple, dict[tuple, DatasetColumnRelationDTO]] = defaultdict(dict)

    # direct lineage (source_column -> target_column)
    for field, raw_column_lineage in target_dataset.facets.columnLineage.fields.items():
        for input_field in raw_column_lineage.inputFields:
            source_dataset_dto = resolve_dataset_ref(input_field, dataset_cache)
            datasets[source_dataset_dto.unique_key] = source_dataset_dto

            dataset_relation_key = (source_dataset_dto.unique_key, target_dataset_dto.unique_key)
            dataset_column_relation = dataset_column_relations[dataset_relation_key]

            for transformation in input_field.transformations:
                # OL integration for Spark before v1.23
                # or with columnLineage.datasetLineageEnabled=false (which is still default)
                # produces INDIRECT lineage for each combination source_column x target_column,
                # which is almost a cartesian product.
                # There are a lot of duplicates here, trying to avoid them by merging items immediately.
                column_relation = DatasetColumnRelationDTO(
                    type=extract_dataset_column_relation_type(transformation),
                    source_column=input_field.field,
                    target_column=field if transformation.type == TRANSFORMATION_TYPE_DIRECT else None,
                )
                column_relation_key = column_relation.unique_key

                existing_column_relation = dataset_column_relation.get(column_relation_key)
                if existing_column_relation:
                    dataset_column_relation[column_relation_key] = existing_column_relation.merge(column_relation)
                else:
                    dataset_column_relation[column_relation_key] = column_relation

    # indirect lineage (source_column -> target_dataset),
    # added to OL since v1.23 and send only when columnLineage.datasetLineageEnabled=true
    for input_field in target_dataset.facets.columnLineage.dataset:
        source_dataset_dto = resolve_dataset_ref(input_field, dataset_cache)
        datasets[source_dataset_dto.unique_key] = source_dataset_dto

        dataset_relation_key = (source_dataset_dto.unique_key, target_dataset_dto.unique_key)
        dataset_column_relation = dataset_column_relations[dataset_relation_key]

        for transformation in input_field.transformations:
            column_relation = DatasetColumnRelationDTO(
                type=extract_dataset_column_relation_type(transformation),
                source_column=input_field.field,
            )
            column_relation_key = column_relation.unique_key

            existing_column_relation = dataset_column_relation.get(column_relation_key)
            if existing_column_relation:
                dataset_column_relation[column_relation_key] = existing_column_relation.merge(column_relation)
            else:
                dataset_column_relation[column_relation_key] = column_relation

    # merge results into DTO objects
    return [
        ColumnLineageDTO(
            operation=operation,
            source_dataset=datasets[source_dataset_dto_key],
            target_dataset=datasets[target_dataset_dto_key],
            dataset_column_relations=list(relations.values()),
        )
        for (source_dataset_dto_key, target_dataset_dto_key), relations in dataset_column_relations.items()
        if dataset_column_relations
    ]
