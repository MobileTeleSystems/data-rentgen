# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import Literal

from faststream.security import SASLPlaintext
from pydantic import Field, SecretStr

from data_rentgen.consumer.settings.security.base import KafkaSecurityBaseSettings


class KafkaSecurityPlaintextSettings(KafkaSecurityBaseSettings):
    """Kafka PLAINTEXT auth settings.

    Examples
    --------

    .. code-block:: bash

        DATA_RENTGEN__KAFKA__SECURITY__TYPE=PLAINTEXT
        DATA_RENTGEN__KAFKA__SECURITY__USER=dummy
        DATA_RENTGEN__KAFKA__SECURITY__PASSWORD=changeme
    """

    type: Literal["PLAINTEXT"] = "PLAINTEXT"
    user: str = Field(description="Kafka security username")
    password: SecretStr = Field(description="Kafka security password")

    def to_security(self):
        return SASLPlaintext(
            self.user,
            self.password.get_secret_value(),
        )
