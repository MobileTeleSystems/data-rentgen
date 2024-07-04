# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from enum import Enum


class Status(str, Enum):  # noqa: WPS600
    STARTED = "STARTED"
    SUCCEEDED = "SUCCEEDED"
    KILLED = "KILLED"
    """Killed externally, e.g. by user request or in case of OOM"""

    FAILED = "FAILED"
    """Internal failure"""

    UNKNOWN = "UNKNOWN"
    """No data about run status"""
