# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import BaseModel, Field


class ApplicationVersionSettings(BaseModel):
    """X-Application-Version Middleware Settings.

    Examples
    --------

    .. code-block:: bash

        ARRAKIS__SERVER__APPLICATION_VERSION__ENABLED=True
        ARRAKIS__SERVER__APPLICATION_VERSION__HEADER_NAME=X-Application-Version
    """

    enabled: bool = Field(default=True, description="Set to ``True`` to enable middleware")
    header_name: str = Field(
        default="X-Application-Version",
        description="Name of response header which is filled up with application version number",
    )
