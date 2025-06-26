# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import json
from typing import Any
from uuid import UUID

from data_rentgen.utils.uuid import generate_static_uuid


def get_fields_digest(fields: Any) -> UUID:
    """Use this function to generate SHA-1 digest of schema fields"""
    content = json.dumps(fields, sort_keys=True)
    return generate_static_uuid(content)
