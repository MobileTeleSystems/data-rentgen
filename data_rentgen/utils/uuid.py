# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Any
from uuid import UUID as OLD_UUID

from pydantic import PlainValidator
from typing_extensions import Annotated
from uuid6 import UUID as UUIDv7


def uuid_version_validator(run_id: Any) -> UUIDv7:
    if isinstance(run_id, str):
        run_id = UUIDv7(run_id)
        if not run_id.version or run_id.version < 6:
            raise ValueError(f"Run ID: {run_id} is not valid uuid. Only UUIDv6+ are supported")
        return run_id
    return run_id


# Teach Pydantic how to parse and represent UUID v7
# Right now use uuid from uuid lib cause: https://github.com/tiangolo/fastapi/issues/10259
UUID = Annotated[
    OLD_UUID,
    PlainValidator(uuid_version_validator),
]
