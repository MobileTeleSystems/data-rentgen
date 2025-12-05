# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(slots=True)
class PaginationDTO(Generic[T]):
    items: list[T]
    page: int
    page_size: int
    total_count: int

    @property
    def has_previous(self) -> bool:
        return self.page > 1

    @property
    def previous_page(self) -> int | None:
        if self.has_previous:
            return self.page - 1
        return None

    @property
    def has_next(self) -> bool:
        previous_items = (self.page - 1) * self.page_size
        return previous_items + len(self.items) < self.total_count

    @property
    def next_page(self) -> int | None:
        if self.has_next:
            return self.page + 1
        return None

    @property
    def pages_count(self) -> int:
        return math.ceil(self.total_count / float(self.page_size)) or 1
