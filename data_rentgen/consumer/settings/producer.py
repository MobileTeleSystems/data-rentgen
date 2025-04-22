# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import BaseModel, Field


class ProducerSettings(BaseModel):
    """Data.Rentgen producer-specific settings.

    These options are passed directly to
    `AIOKafkaProducer <https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.AIOKafkaProducer>`_.

    Examples
    --------

    .. code-block:: bash

        DATA_RENTGEN__PRODUCER__MALFOMED_TOPIC="input.runs__malformed"
    """

    malformed_topic: str = Field(
        default="input.runs__malformed",
        description="Topic to publish malformed messages to.",
    )
