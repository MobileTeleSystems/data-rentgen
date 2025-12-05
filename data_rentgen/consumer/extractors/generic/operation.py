# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import codecs
import re
from abc import ABC, abstractmethod
from textwrap import dedent

from data_rentgen.dto import (
    OperationDTO,
    OperationStatusDTO,
    OperationTypeDTO,
    RunDTO,
    SQLQueryDTO,
)
from data_rentgen.openlineage.run_event import (
    OpenLineageRunEvent,
    OpenLineageRunEventType,
)

# https://www.ascii-code.com/, but left \n intact
ASCII_UNPRINTABLE = re.compile(r"[\x00-\x09\x0b-\x1f\x7f]", re.UNICODE)


def encode_char(char: re.Match[str]) -> str:
    return codecs.encode(char.group(0), "unicode-escape").decode("utf-8")


class OperationExtractorMixin(ABC):
    @abstractmethod
    def extract_run(self, event: OpenLineageRunEvent) -> RunDTO:
        pass

    def extract_operation(self, event: OpenLineageRunEvent) -> OperationDTO:
        """
        Extract OperationDTO from event
        """
        run = self.extract_run(event)
        return self._extract_operation(event, run)

    def _extract_operation(self, event: OpenLineageRunEvent, run: RunDTO) -> OperationDTO:
        operation = OperationDTO(
            id=event.run.runId,  # type: ignore [arg-type]
            run=run,
            name=event.job.name,
            type=self._extract_operation_type(event),
            status=OperationStatusDTO(run.status),
            started_at=run.started_at,
            ended_at=run.ended_at,
            sql_query=self._extract_sql_query(event),
        )
        self._enrich_operation_status(operation, event)
        return operation

    def _extract_operation_type(self, event: OpenLineageRunEvent) -> OperationTypeDTO:
        if event.job.facets.jobType and event.job.facets.jobType.processingType:
            return OperationTypeDTO(event.job.facets.jobType.processingType)
        return OperationTypeDTO.BATCH

    def _enrich_operation_status(self, operation: OperationDTO, event: OpenLineageRunEvent):
        match event.eventType:
            case OpenLineageRunEventType.START:
                operation.started_at = event.eventTime
                operation.status = OperationStatusDTO.STARTED
            case OpenLineageRunEventType.RUNNING:
                operation.status = OperationStatusDTO.STARTED
            case OpenLineageRunEventType.COMPLETE:
                operation.ended_at = event.eventTime
                operation.status = OperationStatusDTO.SUCCEEDED
            case OpenLineageRunEventType.FAIL:
                operation.ended_at = event.eventTime
                operation.status = OperationStatusDTO.FAILED
            case OpenLineageRunEventType.ABORT:
                operation.ended_at = event.eventTime
                operation.status = OperationStatusDTO.KILLED
            case OpenLineageRunEventType.OTHER:
                # OTHER is used only to update run statistics
                pass

        return operation

    def _extract_sql_query(self, event: OpenLineageRunEvent) -> SQLQueryDTO | None:
        if event.job.facets.sql:
            query = dedent(event.job.facets.sql.query).strip()
            # https://stackoverflow.com/questions/56237415/removing-encoding-utf8-0x00-chars-from-pandas-dataframe-for-psycopg2-cursor
            query = ASCII_UNPRINTABLE.sub(encode_char, query)
            return SQLQueryDTO(query=query)
        return None
