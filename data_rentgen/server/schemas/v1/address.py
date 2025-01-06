# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import BaseModel, ConfigDict, Field


class AddressResponseV1(BaseModel):
    url: str = Field(description="Address(URL) of Location")

    model_config = ConfigDict(from_attributes=True)
