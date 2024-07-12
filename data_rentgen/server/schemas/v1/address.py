# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import BaseModel, Field


class AddressResponseV1(BaseModel):
    url: str = Field(description="Address(URL) of Location")

    class Config:
        from_attributes = True
