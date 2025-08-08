# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import textwrap

from pydantic import BaseModel, Field, SecretStr, ValidationInfo, field_validator


class PersonalTokenSettings(BaseModel):
    """Settings for generating and using Personal Tokens.

    Examples
    --------

    .. code-block:: bash

        DATA_RENTGEN__AUTH__PERSONAL_TOKENS__ENABLED=True
        DATA_RENTGEN__AUTH__PERSONAL_TOKENS__SECRET_KEY=somesecret
        DATA_RENTGEN__AUTH__PERSONAL_TOKENS__SECURITY_ALGORITHM=HS256
        DATA_RENTGEN__AUTH__PERSONAL_TOKENS__MAX_DURATION_DAYS=366
    """

    enabled: bool = Field(
        default=True,
        description="Set to ``True`` to allow using Personal Tokens",
    )

    secret_key: SecretStr | None = Field(
        default=None,
        validate_default=True,
        description=textwrap.dedent(
            """
            Secret key for signing Personal Token.

            Can be any string. It is recommended to generate random value for every application instance, e.g.:

            .. code:: shell

                pwgen 32 1
            """,
        ),
    )
    security_algorithm: str = Field(
        default="HS256",
        description=textwrap.dedent(
            """
            Algorithm used for signing Personal Tokens.

            See `pyjwt <https://pyjwt.readthedocs.io/en/latest/algorithms.html>`_
            documentation.
            """,
        ),
    )

    max_duration_days: int = Field(
        default=366,
        description="Maximum duration of Personal Token in days",
    )

    cache_ttl_seconds: int = Field(
        default=300,
        description="Maximum duration of Personal Token cache in seconds",
    )

    cache_size: int = Field(
        default=500,
        description="Maximum number of Personal Tokens in cache",
    )

    @field_validator("secret_key", mode="after")
    @classmethod
    def _check_secret_key(cls, value: SecretStr | None, info: ValidationInfo) -> SecretStr | None:
        if info.data.get("enabled") and not value:
            error_message = "Personal Access Tokens are enabled, but 'secret_key' is not set"
            raise ValueError(error_message)
        return value
