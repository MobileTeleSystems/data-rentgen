# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated, Any
from uuid import UUID as PlainUUID  # noqa: N811

from pydantic import PlainValidator
from uuid6 import UUID as UUIDv7  # noqa: N811


def uuid_version_validator(run_id: Any) -> UUIDv7:
    if isinstance(run_id, str):
        run_id = UUIDv7(run_id)
        if not run_id.version or run_id.version < 6:  # noqa: PLR2004
            err_msg = f"Run ID: {run_id} is not valid uuid. Only UUIDv6+ are supported"
            raise ValueError(err_msg)
        return run_id
    return run_id


# Teach Pydantic how to parse and represent UUID v7
# Right now use uuid from uuid lib cause: https://github.com/tiangolo/fastapi/issues/10259
UUIDv6Plus = Annotated[
    PlainUUID,
    PlainValidator(uuid_version_validator),
]
