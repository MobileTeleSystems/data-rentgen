# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class InputDTO:
    num_rows: int | None = None
    num_bytes: int | None = None
    num_files: int | None = None
