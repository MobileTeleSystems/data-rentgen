# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import secrets
import time
from datetime import datetime, timezone
from hashlib import sha1
from typing import Annotated, Any
from uuid import NAMESPACE_URL, uuid5
from uuid import UUID as BaseUUID  # noqa: N811

from pydantic import PlainValidator
from uuid6 import UUID as NewUUID  # noqa: N811

__all__ = [
    "UUIDv6Plus",
    "extract_timestamp_from_uuid",
    "generate_incremental_uuid",
    "generate_new_uuid",
    "generate_static_uuid",
]


def generate_new_uuid(instant: datetime | None = None) -> NewUUID:
    """Generate new UUID for an instant of time. Each function call returns a new UUID value.

    UUID version is an implementation detail, and **should not** be relied on.
    For now it is `UUIDv7 <https://datatracker.ietf.org/doc/rfc9562/>`_, so for increasing instant values,
    returned UUID is always greater than previous one.

    Using uuid6 lib implementation (MIT License), with few changes:
    * https://github.com/oittaa/uuid6-python/blob/4f879849178b8a7a564f7cb76c3f7a6e5228d9ed/src/uuid6/__init__.py#L128-L147
    * https://github.com/oittaa/uuid6-python/blob/4f879849178b8a7a564f7cb76c3f7a6e5228d9ed/src/uuid6/__init__.py#L46-L51
    """

    timestamp_ms = int(instant.timestamp() * 1000) if instant else time.time_ns() // 10**6
    node = secrets.randbits(76)
    return _build_uuidv7(timestamp_ms, node)


def generate_incremental_uuid(instant: datetime, data: str) -> NewUUID:
    """Generate new UUID for an instant of time and data. Each function call with the same arguments returns the same result.

    UUID version is an implementation detail, and **should not** be relied on.
    For now it is `UUIDv7 <https://datatracker.ietf.org/doc/rfc9562/>`_, so for increasing instant values,
    returned UUID is always greater than previous one.

    Using uuid6 lib implementation (MIT License), with few changes:
    * https://github.com/oittaa/uuid6-python/blob/4f879849178b8a7a564f7cb76c3f7a6e5228d9ed/src/uuid6/__init__.py#L128-L147
    * https://github.com/oittaa/uuid6-python/blob/4f879849178b8a7a564f7cb76c3f7a6e5228d9ed/src/uuid6/__init__.py#L46-L51
    """  # noqa: E501

    instant_utc = instant.astimezone(timezone.utc)
    timestamp_ms = int(instant_utc.timestamp() * 1000)

    # generate the rest of bytes using some hash.
    # can be used to generate consistent UUIDs for some input, e.g. external runId.
    # if data is some static value, e.g. job name, mix it with timestamp to make it more random
    digest = sha1(instant_utc.isoformat().encode("utf-8") + data.encode("utf-8"), usedforsecurity=False).digest()
    # sha1 returns 160bit hash, we need only first 76 bits
    node = int(digest.hex(), 16) >> 84

    return _build_uuidv7(timestamp_ms, node)


def _build_uuidv7(timestamp: int, node: int) -> NewUUID:
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

    return NewUUID(int=uuid_int)


def generate_static_uuid(data: str) -> BaseUUID:
    """Generate static UUID for data. Each function call returns the same UUID value.

    UUID version is an implementation detailed, and **should not** be relied on.
    For now it is UUIDv5 with namespace=URL.
    """
    return uuid5(namespace=NAMESPACE_URL, name=data)  # type: ignore[arg-type]


def extract_timestamp_from_uuid(uuid: BaseUUID) -> datetime:
    """Extract timestamp from UUIDv7"""
    uuid = NewUUID(int=uuid.int)
    if not uuid.version or uuid.version < 6:  # noqa: PLR2004
        msg = "Only UUIDv6+ are supported"
        raise ValueError(msg)
    return datetime.fromtimestamp(uuid.time / 1000, tz=timezone.utc)


def uuid_version_validator(run_id: Any) -> NewUUID:
    if isinstance(run_id, str):
        run_id = NewUUID(run_id)
        if not run_id.version or run_id.version < 6:  # noqa: PLR2004
            err_msg = f"Run ID: {run_id} is not valid uuid. Only UUIDv6+ are supported"
            raise ValueError(err_msg)
        return run_id
    return run_id


# Teach Pydantic how to parse and represent UUID v7
# Right now use uuid from uuid lib cause: https://github.com/tiangolo/fastapi/issues/10259
UUIDv6Plus = Annotated[
    BaseUUID,
    PlainValidator(uuid_version_validator),
]
