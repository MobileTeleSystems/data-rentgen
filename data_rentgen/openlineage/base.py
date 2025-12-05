# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import BaseModel, ConfigDict


class OpenLineageBase(BaseModel):
    """Base class for all OpenLineage models."""

    model_config = ConfigDict(extra="ignore", frozen=True, arbitrary_types_allowed=True)
