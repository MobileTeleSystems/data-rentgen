# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone

from uuid6 import UUID


def extract_created_at_from_uuid(uuid: UUID) -> datetime:
    return datetime.fromtimestamp(uuid.time / 1000, tz=timezone.utc)
