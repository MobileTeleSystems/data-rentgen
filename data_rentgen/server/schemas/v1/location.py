# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import BaseModel, Field

from data_rentgen.server.schemas.v1 import AddressResponseV1


class LocationResponseV1(BaseModel):
    type: str = Field(description="Location type, e.g kafka, hdfs, postgres")
    name: str = Field(description="Location name, e.g. cluster name")
    addresses: list[AddressResponseV1] = Field(description="List of addresses")

    class Config:
        from_attributes = True
