# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import PlainValidator, WithJsonSchema
from typing_extensions import Annotated, Doc
from uuid6 import UUID as UUIDv7

# Teach Pydantic how to parse and represent UUID v7
UUID = Annotated[
    UUIDv7,
    PlainValidator(lambda x: UUIDv7(x) if isinstance(x, str) else x),
    WithJsonSchema({"type": "string", "format": "uuid"}),
    Doc("UUID v7"),
]
