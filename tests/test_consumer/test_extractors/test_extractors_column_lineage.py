import pytest
from uuid6 import UUID

from data_rentgen.consumer.extractors import extract_batch
from data_rentgen.consumer.extractors.column_lineage import (
    extract_column_lineage,
    extract_dataset_column_relation_type,
)
from data_rentgen.consumer.openlineage.dataset_facets import (
    OpenLineageColumnLineageDatasetFacet,
    OpenLineageColumnLineageDatasetFacetField,
    OpenLineageColumnLineageDatasetFacetFieldRef,
    OpenLineageColumnLineageDatasetFacetFieldTransformation,
)
from data_rentgen.dto import (
    ColumnLineageDTO,
    DatasetColumnRelationDTO,
    DatasetColumnRelationTypeDTO,
    DatasetDTO,
)
from tests.test_consumer.test_extractors.fixtures.column_lineage_facets import (
    get_run_event_with_column_lineage,
)
from tests.test_consumer.test_extractors.fixtures.extracted_dto import get_operation_dto


def test_extractors_extract_dataset_column_relation_type_new_type():
    transformation = OpenLineageColumnLineageDatasetFacetFieldTransformation(
        type="SOME_NEW_TYPE",
    )
    dataset_column_relation_type = extract_dataset_column_relation_type(transformation)
    assert dataset_column_relation_type == DatasetColumnRelationTypeDTO.UNKNOWN


@pytest.mark.parametrize("type", ["INDIRECT", "DIRECT"])
def test_extractors_extract_dataset_column_relation_type_no_subtype(type):
    transformation = OpenLineageColumnLineageDatasetFacetFieldTransformation(
        type=type,
    )
    dataset_column_relation_type = extract_dataset_column_relation_type(transformation)
    assert dataset_column_relation_type == DatasetColumnRelationTypeDTO.UNKNOWN


@pytest.mark.parametrize(
    ["type", "subtype", "expected_type"],
    [
        (
            "DIRECT",
            "TRANSFORMATION",
            DatasetColumnRelationTypeDTO.TRANSFORMATION_MASKING,
        ),
        ("DIRECT", "AGGREGATION", DatasetColumnRelationTypeDTO.AGGREGATION_MASKING),
        ("INDIRECT", "JOIN", DatasetColumnRelationTypeDTO.UNKNOWN),
    ],
)
def test_extractors_extract_dataset_column_relation_type_masking(
    type,
    subtype,
    expected_type,
):
    transformation = OpenLineageColumnLineageDatasetFacetFieldTransformation(
        type=type,
        subtype=subtype,
        masking=True,
    )
    dataset_column_relation_type = extract_dataset_column_relation_type(transformation)
    assert dataset_column_relation_type == expected_type


@pytest.mark.parametrize(
    ["type", "subtype", "expected_type"],
    [
        ("DIRECT", "TRANSFORMATION", DatasetColumnRelationTypeDTO.TRANSFORMATION),
        ("INDIRECT", "JOIN", DatasetColumnRelationTypeDTO.JOIN),
        ("INDIRECT", "NEW_SUBTYPE", DatasetColumnRelationTypeDTO.UNKNOWN),
    ],
)
def test_extractors_extract_dataset_column_relation_type_without_masking(
    type,
    subtype,
    expected_type,
):
    transformation = OpenLineageColumnLineageDatasetFacetFieldTransformation(
        type=type,
        subtype=subtype,
        masking=False,
    )
    dataset_column_relation_type = extract_dataset_column_relation_type(transformation)
    assert dataset_column_relation_type == expected_type


def test_extractors_extract_direct_column_lineage(
    extracted_spark_operation,
    extracted_hive_dataset,
    extracted_hdfs_dataset,
    output_event_with_one_to_two_direct_column_lineage,
):
    operation = extracted_spark_operation

    column_lineage = extract_column_lineage(
        operation,
        output_event_with_one_to_two_direct_column_lineage,
    )
    assert column_lineage == [
        ColumnLineageDTO(
            operation=operation,
            source_dataset=extracted_hive_dataset,
            target_dataset=extracted_hdfs_dataset,
            dataset_column_relations=[
                DatasetColumnRelationDTO(
                    type=DatasetColumnRelationTypeDTO.AGGREGATION,
                    source_column="source_col_1",
                    target_column="column_1",
                    fingerprint=None,
                ),
                DatasetColumnRelationDTO(
                    type=DatasetColumnRelationTypeDTO.TRANSFORMATION,
                    source_column="source_col_2",
                    target_column="column_1",
                    fingerprint=None,
                ),
            ],
        ),
    ]


def test_extractors_extract_legacy_indirect_column_lineage(
    extracted_spark_operation,
    extracted_hive_dataset,
    extracted_hdfs_dataset,
    output_event_with_direct_and_legacy_indirect_column_lineage,
):
    """
    The output event contains Indirect column lineage in legacy format (inside 'fields', item).
    This test check's that this data is not included in column lineage.
    """
    operation = extracted_spark_operation

    column_lineage = extract_column_lineage(
        operation,
        output_event_with_direct_and_legacy_indirect_column_lineage,
    )
    assert column_lineage == [
        ColumnLineageDTO(
            operation=operation,
            source_dataset=extracted_hive_dataset,
            target_dataset=extracted_hdfs_dataset,
            dataset_column_relations=[
                DatasetColumnRelationDTO(
                    type=DatasetColumnRelationTypeDTO.AGGREGATION,
                    source_column="source_col_1",
                    target_column="column_1",
                    fingerprint=None,
                ),
                DatasetColumnRelationDTO(
                    type=DatasetColumnRelationTypeDTO.AGGREGATION,
                    source_column="source_col_3",
                    target_column="column_2",
                    fingerprint=None,
                ),
            ],
        ),
    ]


def test_extractors_extract_indirect_column_lineage(
    extracted_spark_operation,
    extracted_hive_dataset,
    extracted_hdfs_dataset,
    output_event_with_one_to_two_direct_and_indirect_column_lineage,
):
    operation = extracted_spark_operation

    column_lineage = extract_column_lineage(
        operation,
        output_event_with_one_to_two_direct_and_indirect_column_lineage,
    )
    assert column_lineage == [
        ColumnLineageDTO(
            operation=operation,
            source_dataset=extracted_hive_dataset,
            target_dataset=extracted_hdfs_dataset,
            dataset_column_relations=[
                DatasetColumnRelationDTO(
                    type=DatasetColumnRelationTypeDTO.AGGREGATION,
                    source_column="source_col_1",
                    target_column="column_1",
                    fingerprint=None,
                ),
                DatasetColumnRelationDTO(
                    type=DatasetColumnRelationTypeDTO.TRANSFORMATION,
                    source_column="source_col_2",
                    target_column="column_1",
                    fingerprint=None,
                ),
                DatasetColumnRelationDTO(
                    type=DatasetColumnRelationTypeDTO.SORT,
                    source_column="source_col_2",
                    target_column=None,
                    fingerprint=None,
                ),
            ],
        ),
    ]


def test_extractors_extract_column_lineage_operations_with_same_lineage(
    extracted_postgres_dataset: DatasetDTO,
    extracted_hive_dataset: DatasetDTO,
):
    column_lineage_facet = OpenLineageColumnLineageDatasetFacet(
        fields={
            "column_1": OpenLineageColumnLineageDatasetFacetField(
                inputFields=[
                    OpenLineageColumnLineageDatasetFacetFieldRef(
                        namespace="postgres://192.168.1.1:5432",
                        name="mydb.myschema.mytable",
                        field="source_col_1",
                        transformations=[
                            OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                type="DIRECT",
                                subtype="AGGREGATION",
                            ),
                        ],
                    ),
                ],
            ),
        },
        dataset=[
            OpenLineageColumnLineageDatasetFacetFieldRef(
                namespace="postgres://192.168.1.1:5432",
                name="mydb.myschema.mytable",
                field="source_col_2",
                transformations=[
                    OpenLineageColumnLineageDatasetFacetFieldTransformation(
                        type="INDIRECT",
                        subtype="SORT",
                        masking=False,
                    ),
                ],
            ),
        ],
    )
    first_operation_id = UUID("0194fae4-6f09-7feb-9285-cea346ec4c14")
    second_operation_id = UUID("0194fae4-e164-7a88-9afb-1d62d7daac95")
    first_event = get_run_event_with_column_lineage(
        first_operation_id,
        column_lineage_facet,
    )
    second_event = get_run_event_with_column_lineage(
        second_operation_id,
        column_lineage_facet,
    )
    extracted = extract_batch([first_event, second_event])
    column_lineage = extracted.column_lineage()

    assert column_lineage == [
        ColumnLineageDTO(
            operation=get_operation_dto(first_operation_id),
            source_dataset=extracted_postgres_dataset,
            target_dataset=extracted_hive_dataset,
            dataset_column_relations=[
                DatasetColumnRelationDTO(
                    type=DatasetColumnRelationTypeDTO.AGGREGATION,
                    source_column="source_col_1",
                    target_column="column_1",
                    fingerprint=None,
                ),
                DatasetColumnRelationDTO(
                    type=DatasetColumnRelationTypeDTO.SORT,
                    source_column="source_col_2",
                    target_column=None,
                    fingerprint=None,
                ),
            ],
        ),
        ColumnLineageDTO(
            operation=get_operation_dto(second_operation_id),
            source_dataset=extracted_postgres_dataset,
            target_dataset=extracted_hive_dataset,
            dataset_column_relations=[
                DatasetColumnRelationDTO(
                    type=DatasetColumnRelationTypeDTO.AGGREGATION,
                    source_column="source_col_1",
                    target_column="column_1",
                    fingerprint=None,
                ),
                DatasetColumnRelationDTO(
                    type=DatasetColumnRelationTypeDTO.SORT,
                    source_column="source_col_2",
                    target_column=None,
                    fingerprint=None,
                ),
            ],
        ),
    ]


def test_extractors_extract_column_lineage_operations_with_transformation_on_same_column(
    extracted_postgres_dataset,
    extracted_hive_dataset,
):
    column_lineage_first_facet = OpenLineageColumnLineageDatasetFacet(
        fields={
            "column": OpenLineageColumnLineageDatasetFacetField(
                inputFields=[
                    OpenLineageColumnLineageDatasetFacetFieldRef(
                        namespace="postgres://192.168.1.1:5432",
                        name="mydb.myschema.mytable",
                        field="source_col",
                        transformations=[
                            OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                type="DIRECT",
                                subtype="AGGREGATION",
                            ),
                        ],
                    ),
                ],
            ),
        },
    )
    column_lineage_second_facet = OpenLineageColumnLineageDatasetFacet(
        fields={
            "column": OpenLineageColumnLineageDatasetFacetField(
                inputFields=[
                    OpenLineageColumnLineageDatasetFacetFieldRef(
                        namespace="postgres://192.168.1.1:5432",
                        name="mydb.myschema.mytable",
                        field="source_col",
                        transformations=[
                            OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                type="DIRECT",
                                subtype="TRANSFORMATION",
                            ),
                            OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                type="DIRECT",
                                subtype="AGGREGATION",
                            ),
                        ],
                    ),
                ],
            ),
        },
    )
    first_operation_id = UUID("0194fae4-6f09-7feb-9285-cea346ec4c14")
    second_operation_id = UUID("0194fae4-e164-7a88-9afb-1d62d7daac95")
    first_event = get_run_event_with_column_lineage(
        first_operation_id,
        column_lineage_first_facet,
    )
    second_event = get_run_event_with_column_lineage(
        second_operation_id,
        column_lineage_second_facet,
    )
    extracted = extract_batch([first_event, second_event])
    column_lineage = extracted.column_lineage()

    assert column_lineage == [
        ColumnLineageDTO(
            operation=get_operation_dto(first_operation_id),
            source_dataset=extracted_postgres_dataset,
            target_dataset=extracted_hive_dataset,
            dataset_column_relations=[
                DatasetColumnRelationDTO(
                    type=DatasetColumnRelationTypeDTO.AGGREGATION,
                    source_column="source_col",
                    target_column="column",
                    fingerprint=None,
                ),
            ],
        ),
        ColumnLineageDTO(
            operation=get_operation_dto(second_operation_id),
            source_dataset=extracted_postgres_dataset,
            target_dataset=extracted_hive_dataset,
            dataset_column_relations=[
                DatasetColumnRelationDTO(
                    type=DatasetColumnRelationTypeDTO.AGGREGATION | DatasetColumnRelationTypeDTO.TRANSFORMATION,
                    source_column="source_col",
                    target_column="column",
                    fingerprint=None,
                ),
            ],
        ),
    ]
