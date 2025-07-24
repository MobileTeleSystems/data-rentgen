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
        DATA_RENTGEN__AUTH__PERSONAL_TOKENS__MAX_DURATION_DAYS=360
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

    @field_validator("secret_key", mode="after")
    @classmethod
    def _check_secret_key(cls, value: SecretStr | None, info: ValidationInfo) -> SecretStr | None:
        if not info.data.get("enabled"):
            return None
        if not value:
            error_message = "Personal Access Tokens are enabled, but 'secret_key' is not set"
            raise ValueError(error_message)
        return value
