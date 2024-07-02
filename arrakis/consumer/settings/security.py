# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import Literal

from faststream.security import BaseSecurity, SASLPlaintext, SASLScram256, SASLScram512
from pydantic import BaseModel, Field, SecretStr, ValidationInfo, field_validator


class KafkaSecuritySettings(BaseModel):
    """Kafka security specific settings.

    Examples
    --------

    .. code-block:: bash

        ARRAKIS__KAFKA__SECURITY__TYPE=scram-256
        ARRAKIS__KAFKA__SECURITY__USER=dummy
        ARRAKIS__KAFKA__SECURITY__PASSWORD=changeme
    """

    type: Literal["plaintext", "scram-sha256", "scram-sha512"] | None = Field(
        default=None,
        description="Kafka security type",
    )
    user: str | None = Field(default=None, description="Kafka security username")
    password: SecretStr | None = Field(default=None, description="Kafka security password")

    @field_validator("user", "password", mode="after")
    @classmethod
    def check_security(cls, value: str, info: ValidationInfo):
        security_type = info.data.get("type")
        if security_type is None:
            return value
        if value is None:
            raise ValueError(f"User or password is required for security type {security_type!r}")
        return value


def get_broker_security(settings: KafkaSecuritySettings) -> BaseSecurity:
    if not settings.type:
        return BaseSecurity()

    security_class: type[BaseSecurity]
    match settings.type:
        case "plaintext":
            security_class = SASLPlaintext
        case "scram-sha256":
            security_class = SASLScram256
        case "scram-sha512":
            security_class = SASLScram512

    return security_class(settings.user, settings.password.get_secret_value())  # type: ignore[union-attr, arg-type]
