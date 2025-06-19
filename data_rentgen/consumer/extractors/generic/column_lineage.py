# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict

from data_rentgen.consumer.openlineage.dataset import (
    OpenLineageDataset,
)
from data_rentgen.consumer.openlineage.dataset_facets import (
    OpenLineageColumnLineageDatasetFacetField,
    OpenLineageColumnLineageDatasetFacetFieldRef,
    OpenLineageColumnLineageDatasetFacetFieldTransformation,
    OpenLineageSymlinkIdentifier,
)
from data_rentgen.dto import (
    ColumnLineageDTO,
    DatasetColumnRelationDTO,
    DatasetColumnRelationTypeDTO,
    DatasetDTO,
    OperationDTO,
)

TRANSFORMATION_TYPE_DIRECT = "DIRECT"
TRANSFORMATION_TYPE_INDIRECT = "INDIRECT"

TRANSFORMATION_SUBTYPE_MAP_MASKING = {
    "MASKED": DatasetColumnRelationTypeDTO.TRANSFORMATION_MASKING,
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


class ColumnLineageExtractorMixin(ABC):
    def __init__(self):
        self._dataset_ref_to_dto_cache: dict[tuple[str, str], DatasetDTO] = {}

    @abstractmethod
    def _extract_dataset_ref(
        self,
        dataset: OpenLineageDataset | OpenLineageColumnLineageDatasetFacetFieldRef | OpenLineageSymlinkIdentifier,
    ) -> DatasetDTO:
        pass

    def _resolve_dataset_ref(
        self,
        dataset_ref: OpenLineageDataset | OpenLineageColumnLineageDatasetFacetFieldRef | OpenLineageSymlinkIdentifier,
    ) -> DatasetDTO:
        """
        Column lineage has a lot of dataset references, so this is a hot path which requires caching.
        """
        dataset_cache_key = (dataset_ref.namespace, dataset_ref.name)
        if dataset_cache_key not in self._dataset_ref_to_dto_cache:
            self._dataset_ref_to_dto_cache[dataset_cache_key] = self._extract_dataset_ref(dataset_ref)
        return self._dataset_ref_to_dto_cache[dataset_cache_key]

    def extract_column_lineage(
        self,
        operation: OperationDTO,
        output_dataset: OpenLineageDataset,
    ) -> list[ColumnLineageDTO]:
        """
        Extract ColumnLineageDTO from output dataset, and bound to operation
        """
        if not output_dataset.facets.columnLineage:
            return []

        output_dataset_dto = self._resolve_dataset_ref(output_dataset)

        # Grouping column lineage by source+target dataset. This is unique combination within operation,
        # so we can use it to generate the same fingerprint for all dataset column relations
        datasets = {output_dataset_dto.unique_key: output_dataset_dto}
        dataset_column_relations: dict[tuple, dict[tuple, DatasetColumnRelationDTO]] = defaultdict(dict)

        # direct lineage (source_column -> target_column)
        for field, raw_column_lineage in output_dataset.facets.columnLineage.fields.items():
            for input_field in raw_column_lineage.inputFields:
                source_dataset_dto = self._resolve_dataset_ref(input_field)
                datasets[source_dataset_dto.unique_key] = source_dataset_dto

                dataset_relation_key = (source_dataset_dto.unique_key, output_dataset_dto.unique_key)
                dataset_column_relation = dataset_column_relations[dataset_relation_key]

                transformations = input_field.transformations or [self._legacy_transformation(raw_column_lineage)]
                for transformation in transformations:
                    # OL integration for Spark before v1.23
                    # or with columnLineage.datasetLineageEnabled=false (which is still default)
                    # produces INDIRECT lineage for each combination source_column x target_column,
                    # which is almost a cartesian product.
                    # There are a lot of duplicates here, trying to avoid them by merging items immediately.
                    column_relation = DatasetColumnRelationDTO(
                        type=self.extract_dataset_column_relation_type(transformation),
                        source_column=input_field.field,
                        target_column=field if transformation.type == TRANSFORMATION_TYPE_DIRECT else None,
                    )
                    column_relation_key = column_relation.unique_key

                    existing_column_relation = dataset_column_relation.get(column_relation_key)
                    if existing_column_relation:
                        dataset_column_relation[column_relation_key] = existing_column_relation.merge(column_relation)
                    else:
                        dataset_column_relation[column_relation_key] = column_relation

        # indirect lineage (source_column -> output_dataset),
        # added to OL since v1.23 and send only when columnLineage.datasetLineageEnabled=true
        for input_field in output_dataset.facets.columnLineage.dataset:
            source_dataset_dto = self._resolve_dataset_ref(input_field)
            datasets[source_dataset_dto.unique_key] = source_dataset_dto

            dataset_relation_key = (source_dataset_dto.unique_key, output_dataset_dto.unique_key)
            dataset_column_relation = dataset_column_relations[dataset_relation_key]

            transformations = input_field.transformations or [
                OpenLineageColumnLineageDatasetFacetFieldTransformation(type=TRANSFORMATION_TYPE_INDIRECT),
            ]

            for transformation in transformations:
                column_relation = DatasetColumnRelationDTO(
                    type=self.extract_dataset_column_relation_type(transformation),
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
                target_dataset=datasets[output_dataset_dto_key],
                dataset_column_relations=list(relations.values()),
            )
            for (source_dataset_dto_key, output_dataset_dto_key), relations in dataset_column_relations.items()
            if dataset_column_relations
        ]

    def extract_dataset_column_relation_type(
        self,
        transformation: OpenLineageColumnLineageDatasetFacetFieldTransformation,
    ) -> DatasetColumnRelationTypeDTO:
        result: DatasetColumnRelationTypeDTO | None = None
        if transformation.subtype:
            if transformation.masking:
                result = TRANSFORMATION_SUBTYPE_MAP_MASKING.get(transformation.subtype)
            else:
                result = TRANSFORMATION_SUBTYPE_MAP.get(transformation.subtype)

        return result or DatasetColumnRelationTypeDTO.UNKNOWN

    def _legacy_transformation(self, field: OpenLineageColumnLineageDatasetFacetField):
        type_ = field.transformationType or ""
        return OpenLineageColumnLineageDatasetFacetFieldTransformation(
            type=TRANSFORMATION_TYPE_INDIRECT if type_ == TRANSFORMATION_TYPE_INDIRECT else TRANSFORMATION_TYPE_DIRECT,
            subtype=type_,
            description=field.transformationDescription,
            masking="mask" in type_.lower(),
        )
