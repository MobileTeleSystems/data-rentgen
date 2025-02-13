# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import BaseModel, ConfigDict, Field, ImportString


class AuthSettings(BaseModel):
    """Authorization-related settings.

    Here you can set auth provider class along with its options.

    Examples
    --------

    .. code-block:: bash

        DATA_RENTGEN__AUTH__PROVIDER=data_rentgen.server.providers.auth.dummy_provider.DummyAuthProvider

        # pass access_key.secret_key = "secret" to DummyAuthProviderSettings
        DATA_RENTGEN__AUTH__ACCESS_KEY__SECRET_KEY=secret
    """

    provider: ImportString = Field(  # type: ignore[assignment]
        default="data_rentgen.server.providers.auth.dummy_provider.DummyAuthProvider",
        description="Full name of auth provider class",
        validate_default=True,
    )

    model_config = ConfigDict(extra="allow")
