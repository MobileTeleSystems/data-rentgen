# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import textwrap
from enum import Enum

from pydantic import BaseModel, Field

from data_rentgen.consumer.settings.security import KafkaSecuritySettings


class KafkaCompression(str, Enum):
    GZIP = "gzip"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ZSTD = "zstd"

    def __str__(self):
        return self.value


class KafkaSettings(BaseModel):
    """Data.Rentgen consumer Kafka-specific settings.

    These options are passed directly to
    `AIOKafkaConsumer <https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.AIOKafkaConsumer>`_.

    Examples
    --------

    .. code-block:: bash

        DATA_RENTGEN__KAFKA__BOOTSTRAP_SERVERS=localhost:9092
        DATA_RENTGEN__KAFKA__SECURITY__TYPE=scram-256
        DATA_RENTGEN__KAFKA__REQUEST_TIMEOUT_MS=5000
        DATA_RENTGEN__KAFKA__CONNECTIONS_MAX_IDLE_MS=540000
    """

    bootstrap_servers: str = Field(
        description="List of Kafka bootstrap servers.",
        min_length=1,
    )
    security: KafkaSecuritySettings = Field(
        default_factory=KafkaSecuritySettings,
        description="Kafka security settings.",
    )
    compression: KafkaCompression | None = Field(
        default=None,
        description="Kafka message compression type.",
    )
    # Defaults are copied from FastStream: https://github.com/airtai/faststream/blob/0.5.33/faststream/kafka/fastapi/fastapi.py#L78
    # But only options, related to consuming messages
    request_timeout_ms: int = Field(
        default=40 * 1000,
        description="Client request timeout in milliseconds.",
    )
    retry_backoff_ms: int = Field(
        default=100,
        description="Milliseconds to backoff when retrying on errors.",
    )
    metadata_max_age_ms: int = Field(
        default=5 * 60 * 1000,
        description=textwrap.dedent(
            """
            The period of time in milliseconds after which we force a refresh of metadata,
            even if we haven't seen any partition leadership changes,
            to proactively discover any new brokers or partitions.
            """,
        ),
    )
    connections_max_idle_ms: int = Field(
        default=9 * 60 * 1000,
        description=textwrap.dedent(
            """
            Close idle connections after the number of milliseconds specified by this config.
            Specifying ``None`` will disable idle checks.
            """,
        ),
    )
