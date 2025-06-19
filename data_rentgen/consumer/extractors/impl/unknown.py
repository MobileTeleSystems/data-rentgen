# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from data_rentgen.consumer.extractors.generic import GenericExtractor
from data_rentgen.consumer.openlineage.run_event import OpenLineageRunEvent


class UnknownExtractor(GenericExtractor):
    """
    Extractor for OpenLineage events produced by unknown integrations.
    Should always be last extractor in the chain, as it matches any event.
    """

    def match(self, event: OpenLineageRunEvent) -> bool:
        return True

    def is_operation(self, event: OpenLineageRunEvent) -> bool:
        # This is heuristics which prevents creating useless operations
        return bool(event.inputs or event.outputs)
