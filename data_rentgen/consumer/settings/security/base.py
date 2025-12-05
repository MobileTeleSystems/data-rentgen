# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import Any

from faststream.security import BaseSecurity
from pydantic import BaseModel


class KafkaSecurityBaseSettings(BaseModel):
    def to_security(self) -> BaseSecurity:
        return BaseSecurity()

    def extra_broker_kwargs(self) -> dict[str, Any]:
        return {}

    async def initialize(self) -> None:
        return

    async def destroy(self) -> None:
        return

    async def refresh(self) -> None:
        return
