# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import BaseModel, Field

from arrakis.consumer.settings.security import KafkaSecuritySettings


class KafkaSettings(BaseModel):
    """Arrakis consumer Kafka-specific settings.

    Examples
    --------

    .. code-block:: bash

        ARRAKIS__KAFKA__BOOTSTRAP_SERVERS=localhost:9092
        ARRAKIS__KAFKA__SECURITY__TYPE=scram-256
    """

    bootstrap_servers: str = Field(
        description="List of Kafka bootstrap servers",
        min_length=1,
    )
    security: KafkaSecuritySettings = Field(
        default_factory=KafkaSecuritySettings,
        description=":ref:`Kafka security settings <configuration-consumer-kafka-security>`",
    )
