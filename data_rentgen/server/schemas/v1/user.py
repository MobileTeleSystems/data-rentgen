# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import BaseModel, ConfigDict, Field


class UserResponseV1(BaseModel):
    name: str = Field(description="User name")
    model_config = ConfigDict(from_attributes=True)
