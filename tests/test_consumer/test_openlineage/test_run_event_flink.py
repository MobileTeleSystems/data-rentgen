from datetime import datetime, timezone

import pytest
from packaging.version import Version
from pydantic import TypeAdapter
from uuid6 import UUID

from data_rentgen.consumer.openlineage.dataset import (
    OpenLineageInputDataset,
    OpenLineageOutputDataset,
)
from data_rentgen.consumer.openlineage.dataset_facets import (
    OpenLineageDatasetFacets,
    OpenLineageDocumentationDatasetFacet,
    OpenLineageSchemaDatasetFacet,
    OpenLineageSchemaField,
    OpenLineageSymlinkIdentifier,
    OpenLineageSymlinksDatasetFacet,
    OpenLineageSymlinkType,
)
from data_rentgen.consumer.openlineage.job import OpenLineageJob
from data_rentgen.consumer.openlineage.job_facets import (
    OpenLineageJobFacets,
    OpenLineageJobProcessingType,
    OpenLineageJobTypeJobFacet,
)
from data_rentgen.consumer.openlineage.run import OpenLineageRun
from data_rentgen.consumer.openlineage.run_event import (
    OpenLineageRunEvent,
    OpenLineageRunEventType,
)
from data_rentgen.consumer.openlineage.run_facets import (
    OpenLineageFlinkJobDetailsRunFacet,
    OpenLineageProcessingEngineRunFacet,
    OpenLineageRunFacets,
)

RunEventAdapter = TypeAdapter(OpenLineageRunEvent)


@pytest.mark.parametrize(
    ["processing_type", "expected_job_type"],
    [
        ("STREAMING", OpenLineageJobProcessingType.STREAMING),
        ("BATCH", OpenLineageJobProcessingType.BATCH),
    ],
)
def test_run_event_flink_job_start(processing_type: str, expected_job_type: OpenLineageJobProcessingType):
    json = {
        "eventTime": "2025-04-22T08:37:53.938068Z",
        "producer": "https://github.com/OpenLineage/OpenLineage/tree/1.34.0/integration/flink",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "eventType": "START",
        "run": {
            "runId": "01965ca5-85bc-789d-93a1-9e4e48bd37b3",
            "facets": {
                "flink_job": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.34.0/integration/flink",
                    "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
                    "jobId": "b825f524-49d6-4dd8-bffd-3e5742c528d0",
                },
                "processing_engine": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.34.0/integration/flink",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ProcessingEngineRunFacet.json#/$defs/ProcessingEngineRunFacet",
                    "version": "1.19.0",
                    "name": "flink",
                    "openlineageAdapterVersion": "1.34.0-SNAPSHOT",
                },
            },
        },
        "job": {
            "namespace": "http://localhost:18081",
            "name": "flink_job",
            "facets": {
                "jobType": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.34.0/integration/flink",
                    "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
                    "processingType": processing_type,
                    "integration": "FLINK",
                    "jobType": "JOB",
                },
            },
        },
        "inputs": [
            {
                "namespace": "kafka://kafka-host1:9092",
                "name": "input_topic",
                "facets": {
                    "documentation": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.34.0/integration/flink",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DocumentationDatasetFacet.json#/$defs/DocumentationDatasetFacet",
                        "description": "My Complex Table",
                    },
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.34.0/integration/flink",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                        "fields": [
                            {
                                "name": "price",
                                "type": "DECIMAL(38, 18)",
                                "description": "",
                            },
                            {
                                "name": "currency",
                                "type": "STRING",
                                "description": "",
                            },
                            {
                                "name": "log_date",
                                "type": "DATE",
                                "description": "",
                            },
                            {
                                "name": "log_time",
                                "type": "TIME(0)",
                                "description": "",
                            },
                            {
                                "name": "log_ts",
                                "type": "TIMESTAMP(3)",
                                "description": "",
                            },
                        ],
                    },
                    "symlinks": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.34.0/integration/flink",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json#/$defs/SymlinksDatasetFacet",
                        "identifiers": [
                            {
                                "namespace": "kafka://kafka-host1:9092",
                                "name": "default_catalog.default_database.kafka_input",
                                "type": "TABLE",
                            },
                        ],
                    },
                },
            },
        ],
        "outputs": [
            {
                "namespace": "kafka://kafka-host2:9092",
                "name": "output_topic",
                "facets": {
                    "documentation": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.34.0/integration/flink",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DocumentationDatasetFacet.json#/$defs/DocumentationDatasetFacet",
                        "description": "",
                    },
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.34.0/integration/flink",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                        "fields": [
                            {
                                "name": "ts_interval",
                                "type": "STRING",
                                "description": "",
                            },
                            {
                                "name": "max_log_date",
                                "type": "STRING",
                                "description": "",
                            },
                            {
                                "name": "max_log_time",
                                "type": "STRING",
                                "description": "",
                            },
                            {
                                "name": "max_ts",
                                "type": "STRING",
                                "description": "",
                            },
                            {
                                "name": "counter",
                                "type": "BIGINT",
                                "description": "",
                            },
                            {
                                "name": "max_price",
                                "type": "DECIMAL(38, 18)",
                                "description": "",
                            },
                        ],
                    },
                    "symlinks": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.34.0/integration/flink",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json#/$defs/SymlinksDatasetFacet",
                        "identifiers": [
                            {
                                "namespace": "kafka://kafka-host2:9092",
                                "name": "default_catalog.default_database.kafka_output",
                                "type": "TABLE",
                            },
                        ],
                    },
                },
            },
        ],
    }

    assert RunEventAdapter.validate_python(json) == OpenLineageRunEvent(
        eventTime=datetime(2025, 4, 22, 8, 37, 53, 938068, tzinfo=timezone.utc),
        eventType=OpenLineageRunEventType.START,
        job=OpenLineageJob(
            namespace="http://localhost:18081",
            name="flink_job",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=expected_job_type,
                    integration="FLINK",
                    jobType="JOB",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=UUID("01965ca5-85bc-789d-93a1-9e4e48bd37b3"),
            facets=OpenLineageRunFacets(
                # https://github.com/OpenLineage/OpenLineage/pull/3744
                flink_job=OpenLineageFlinkJobDetailsRunFacet(
                    jobId="b825f524-49d6-4dd8-bffd-3e5742c528d0",
                ),
                # https://github.com/OpenLineage/OpenLineage/pull/3726
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("1.19.0"),
                    name="flink",
                    openlineageAdapterVersion=Version("1.34.0"),
                ),
            ),
        ),
        inputs=[
            OpenLineageInputDataset(
                namespace="kafka://kafka-host1:9092",
                name="input_topic",
                facets=OpenLineageDatasetFacets(
                    documentation=OpenLineageDocumentationDatasetFacet(description="My Complex Table"),
                    schema=OpenLineageSchemaDatasetFacet(
                        fields=[
                            OpenLineageSchemaField(
                                name="price",
                                type="DECIMAL(38, 18)",
                                description="",
                            ),
                            OpenLineageSchemaField(
                                name="currency",
                                type="STRING",
                                description="",
                            ),
                            OpenLineageSchemaField(
                                name="log_date",
                                type="DATE",
                                description="",
                            ),
                            OpenLineageSchemaField(
                                name="log_time",
                                type="TIME(0)",
                                description="",
                            ),
                            OpenLineageSchemaField(
                                name="log_ts",
                                type="TIMESTAMP(3)",
                                description="",
                            ),
                        ],
                    ),
                    # https://github.com/OpenLineage/OpenLineage/pull/3657
                    symlinks=OpenLineageSymlinksDatasetFacet(
                        identifiers=[
                            OpenLineageSymlinkIdentifier(
                                namespace="kafka://kafka-host1:9092",
                                name="default_catalog.default_database.kafka_input",
                                type=OpenLineageSymlinkType.TABLE,
                            ),
                        ],
                    ),
                ),
            ),
        ],
        outputs=[
            OpenLineageOutputDataset(
                namespace="kafka://kafka-host2:9092",
                name="output_topic",
                facets=OpenLineageDatasetFacets(
                    documentation=OpenLineageDocumentationDatasetFacet(description=""),
                    schema=OpenLineageSchemaDatasetFacet(
                        fields=[
                            OpenLineageSchemaField(
                                name="ts_interval",
                                type="STRING",
                                description="",
                            ),
                            OpenLineageSchemaField(
                                name="max_log_date",
                                type="STRING",
                                description="",
                            ),
                            OpenLineageSchemaField(
                                name="max_log_time",
                                type="STRING",
                                description="",
                            ),
                            OpenLineageSchemaField(
                                name="max_ts",
                                type="STRING",
                                description="",
                            ),
                            OpenLineageSchemaField(
                                name="counter",
                                type="BIGINT",
                                description="",
                            ),
                            OpenLineageSchemaField(
                                name="max_price",
                                type="DECIMAL(38, 18)",
                                description="",
                            ),
                        ],
                    ),
                    # https://github.com/OpenLineage/OpenLineage/pull/3657
                    symlinks=OpenLineageSymlinksDatasetFacet(
                        identifiers=[
                            OpenLineageSymlinkIdentifier(
                                namespace="kafka://kafka-host2:9092",
                                name="default_catalog.default_database.kafka_output",
                                type=OpenLineageSymlinkType.TABLE,
                            ),
                        ],
                    ),
                ),
            ),
        ],
    )


def test_run_event_flink_job_start_before_openlineage_1_34():
    json = {
        "eventTime": "2025-04-22T08:37:53.938068Z",
        "producer": "https://github.com/OpenLineage/OpenLineage/tree/ 1.33.0/integration/flink",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "eventType": "START",
        "run": {
            "runId": "01965ca5-85bc-789d-93a1-9e4e48bd37b3",
            "facets": {},
        },
        "job": {
            "namespace": "http://localhost:18081",
            "name": "flink_job",
            "facets": {
                "jobType": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/ 1.33.0/integration/flink",
                    "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
                    "processingType": "BATCH",
                    "integration": "FLINK",
                    "jobType": "JOB",
                },
            },
        },
        "inputs": [
            {
                "namespace": "kafka://kafka-host1:9092",
                "name": "input_topic",
                "facets": {
                    "documentation": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/ 1.33.0/integration/flink",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DocumentationDatasetFacet.json#/$defs/DocumentationDatasetFacet",
                        "description": "My Complex Table",
                    },
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/ 1.33.0/integration/flink",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                        "fields": [
                            {
                                "name": "price",
                                "type": "DECIMAL(38, 18)",
                                "description": "",
                            },
                            {
                                "name": "currency",
                                "type": "STRING",
                                "description": "",
                            },
                            {
                                "name": "log_date",
                                "type": "DATE",
                                "description": "",
                            },
                            {
                                "name": "log_time",
                                "type": "TIME(0)",
                                "description": "",
                            },
                            {
                                "name": "log_ts",
                                "type": "TIMESTAMP(3)",
                                "description": "",
                            },
                        ],
                    },
                    "symlinks": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/ 1.33.0/integration/flink",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json#/$defs/SymlinksDatasetFacet",
                        "identifiers": [
                            {
                                "namespace": "kafka://kafka-host1:9092",
                                "name": "default_catalog.default_database.kafka_input",
                                "type": "TABLE",
                            },
                        ],
                    },
                },
            },
        ],
        "outputs": [
            {
                "namespace": "kafka://kafka-host2:9092",
                "name": "output_topic",
                "facets": {
                    "documentation": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/ 1.33.0/integration/flink",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DocumentationDatasetFacet.json#/$defs/DocumentationDatasetFacet",
                        "description": "",
                    },
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/ 1.33.0/integration/flink",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                        "fields": [
                            {
                                "name": "ts_interval",
                                "type": "STRING",
                                "description": "",
                            },
                            {
                                "name": "max_log_date",
                                "type": "STRING",
                                "description": "",
                            },
                            {
                                "name": "max_log_time",
                                "type": "STRING",
                                "description": "",
                            },
                            {
                                "name": "max_ts",
                                "type": "STRING",
                                "description": "",
                            },
                            {
                                "name": "counter",
                                "type": "BIGINT",
                                "description": "",
                            },
                            {
                                "name": "max_price",
                                "type": "DECIMAL(38, 18)",
                                "description": "",
                            },
                        ],
                    },
                    "symlinks": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/ 1.33.0/integration/flink",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json#/$defs/SymlinksDatasetFacet",
                        "identifiers": [
                            {
                                "namespace": "kafka://kafka-host2:9092",
                                "name": "default_catalog.default_database.kafka_output",
                                "type": "TABLE",
                            },
                        ],
                    },
                },
            },
        ],
    }

    assert RunEventAdapter.validate_python(json) == OpenLineageRunEvent(
        eventTime=datetime(2025, 4, 22, 8, 37, 53, 938068, tzinfo=timezone.utc),
        eventType=OpenLineageRunEventType.START,
        job=OpenLineageJob(
            namespace="http://localhost:18081",
            name="flink_job",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="FLINK",
                    jobType="JOB",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=UUID("01965ca5-85bc-789d-93a1-9e4e48bd37b3"),
        ),
        inputs=[
            OpenLineageInputDataset(
                namespace="kafka://kafka-host1:9092",
                name="input_topic",
                facets=OpenLineageDatasetFacets(
                    documentation=OpenLineageDocumentationDatasetFacet(description="My Complex Table"),
                    schema=OpenLineageSchemaDatasetFacet(
                        fields=[
                            OpenLineageSchemaField(
                                name="price",
                                type="DECIMAL(38, 18)",
                                description="",
                            ),
                            OpenLineageSchemaField(
                                name="currency",
                                type="STRING",
                                description="",
                            ),
                            OpenLineageSchemaField(
                                name="log_date",
                                type="DATE",
                                description="",
                            ),
                            OpenLineageSchemaField(
                                name="log_time",
                                type="TIME(0)",
                                description="",
                            ),
                            OpenLineageSchemaField(
                                name="log_ts",
                                type="TIMESTAMP(3)",
                                description="",
                            ),
                        ],
                    ),
                    # https://github.com/OpenLineage/OpenLineage/pull/3657
                    symlinks=OpenLineageSymlinksDatasetFacet(
                        identifiers=[
                            OpenLineageSymlinkIdentifier(
                                namespace="kafka://kafka-host1:9092",
                                name="default_catalog.default_database.kafka_input",
                                type=OpenLineageSymlinkType.TABLE,
                            ),
                        ],
                    ),
                ),
            ),
        ],
        outputs=[
            OpenLineageOutputDataset(
                namespace="kafka://kafka-host2:9092",
                name="output_topic",
                facets=OpenLineageDatasetFacets(
                    documentation=OpenLineageDocumentationDatasetFacet(description=""),
                    schema=OpenLineageSchemaDatasetFacet(
                        fields=[
                            OpenLineageSchemaField(
                                name="ts_interval",
                                type="STRING",
                                description="",
                            ),
                            OpenLineageSchemaField(
                                name="max_log_date",
                                type="STRING",
                                description="",
                            ),
                            OpenLineageSchemaField(
                                name="max_log_time",
                                type="STRING",
                                description="",
                            ),
                            OpenLineageSchemaField(
                                name="max_ts",
                                type="STRING",
                                description="",
                            ),
                            OpenLineageSchemaField(
                                name="counter",
                                type="BIGINT",
                                description="",
                            ),
                            OpenLineageSchemaField(
                                name="max_price",
                                type="DECIMAL(38, 18)",
                                description="",
                            ),
                        ],
                    ),
                    # https://github.com/OpenLineage/OpenLineage/pull/3657
                    symlinks=OpenLineageSymlinksDatasetFacet(
                        identifiers=[
                            OpenLineageSymlinkIdentifier(
                                namespace="kafka://kafka-host2:9092",
                                name="default_catalog.default_database.kafka_output",
                                type=OpenLineageSymlinkType.TABLE,
                            ),
                        ],
                    ),
                ),
            ),
        ],
    )
