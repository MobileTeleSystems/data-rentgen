# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class PingResponse(BaseModel):
    """Ping result"""

    status: Literal["ok"] = "ok"
