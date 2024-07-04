# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import secrets
import time
from datetime import datetime
from uuid import NAMESPACE_URL
from uuid import UUID as UUIDv5
from uuid import uuid5

from uuid6 import UUID as UUIDv7

__all__ = ["generate_new_uuid", "generate_static_uuid"]


def generate_new_uuid(instant: datetime | None = None) -> UUIDv7:
    """Generate new UUID for an instant of time. Each function call returns a new UUID value.

    UUID version is an implementation detail, and **should not** be relied on.
    For now it is `UUIDv7 <https://datatracker.ietf.org/doc/rfc9562/>`_, so for increasing instant values,
    returned UUID is always greater than previous one.

    Using uuid6 lib implementation (MIT License), with few changes:
    * https://github.com/oittaa/uuid6-python/blob/4f879849178b8a7a564f7cb76c3f7a6e5228d9ed/src/uuid6/__init__.py#L128-L147
    * https://github.com/oittaa/uuid6-python/blob/4f879849178b8a7a564f7cb76c3f7a6e5228d9ed/src/uuid6/__init__.py#L46-L51
    """

    timestamp = int(instant.timestamp() * 1000) if instant else time.time_ns() // 10**6
    node = secrets.randbits(76)

    # merge timestamp and node into 128-bit UUID
    # timestamp is first 48 bits, node is last 80 bits
    uuid_int = (timestamp & 0xFFFFFFFFFFFF) << 80
    uuid_int |= node & 0xFFFFFFFFFFFFFFFFFFFF

    # Set the version number (4 bit).
    version = 7
    uuid_int &= ~(0xF000 << 64)
    uuid_int |= version << 76

    # Set the variant (2 bit) to RFC 4122.
    uuid_int &= ~(0xC000 << 48)
    uuid_int |= 0x8000 << 48

    return UUIDv7(int=uuid_int)


def generate_static_uuid(data: str) -> UUIDv5:
    """Generate static UUID for data. Each function call returns the same UUID value.

    UUID version is an implementation detailed, and **should not** be relied on.
    For now it is UUIDv5 with namespace=URL.
    """
    return uuid5(namespace=NAMESPACE_URL, name=data)
