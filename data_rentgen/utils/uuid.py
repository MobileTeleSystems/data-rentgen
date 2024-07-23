# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Any

from pydantic import PlainValidator, WithJsonSchema
from typing_extensions import Annotated, Doc
from uuid6 import UUID as UUIDv7


def uuid_version_validator(run_id: Any) -> Any:
    if isinstance(run_id, str):
        run_id = UUIDv7(run_id)
        if not run_id.version or run_id.version < 6:
            raise ValueError(f"Run ID: {run_id} is not valid uuid. Only UUIDv6+ are supported")
        return run_id
    return run_id


# Teach Pydantic how to parse and represent UUID v7
UUID = Annotated[
    UUIDv7,
    PlainValidator(uuid_version_validator),
    WithJsonSchema({"type": "string", "format": "uuid"}),
    Doc("UUID v7"),
]
