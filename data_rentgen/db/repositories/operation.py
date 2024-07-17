# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from uuid import UUID

from sqlalchemy import select

from data_rentgen.db.models import Operation, OperationType, Status
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.uuid import extract_timestamp_from_uuid
from data_rentgen.dto import OperationDTO


class OperationRepository(Repository[Operation]):
    async def create_or_update(self, operation: OperationDTO, run_id: UUID | None) -> Operation:
        created_at = extract_timestamp_from_uuid(operation.id)
        query = select(Operation).where(
            Operation.created_at == created_at,
            Operation.id == operation.id,
        )
        result = await self._session.scalar(query)
        if not result:
            result = Operation(
                created_at=created_at,
                id=operation.id,
                run_id=run_id,
                name=operation.name,
                type=OperationType(operation.type) if operation.type else OperationType.BATCH,
                status=Status(operation.status) if operation.status else Status.UNKNOWN,
                started_at=operation.started_at,
                ended_at=operation.ended_at,
                description=operation.description,
                position=operation.position,
            )
            self._session.add(result)
        else:
            optional_fields = {
                # Operation run_id and type are not null while operation is created, but may be empty in later events
                "type": OperationType(operation.type) if operation.type else None,
                "status": Status(operation.status) if operation.status else None,
                "started_at": operation.started_at,
                "ended_at": operation.ended_at,
                "description": operation.description,
                "position": operation.position,
            }
            for column, value in optional_fields.items():
                if value is not None:
                    setattr(result, column, value)

        await self._session.flush([result])
        return result
