# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from pydantic import BaseModel, Field, field_validator


class StaticFilesSettings(BaseModel):
    """Static files serving settings.

    Files are served at ``/static`` endpoint.

    Examples
    --------

    .. code-block:: bash

        ARRAKIS__SERVER__STATIC_FILES__ENABLED=True
        ARRAKIS__SERVER__STATIC_FILES__DIRECTORY=/app/arrakis/server/static
    """

    enabled: bool = Field(default=True, description="Set to ``True`` to enable static file serving")
    directory: Path = Field(
        default=Path("docs/_static"),
        description="Directory containing static files",
    )

    @field_validator("directory")
    def _validate_directory(cls, value: Path) -> Path:
        if not value.exists():
            raise ValueError(f"Directory '{value}' does not exist")
        if not value.is_dir():
            raise ValueError(f"Path '{value}' is not a directory")
        return value
