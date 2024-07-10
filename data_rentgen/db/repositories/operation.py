# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from uuid import UUID

from sqlalchemy.dialects.postgresql import insert

from data_rentgen.db.models import Operation, OperationType, Status
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import OperationDTO


class OperationRepository(Repository[Operation]):
    async def create_or_update(self, operation: OperationDTO, run_id: UUID | None) -> Operation:
        mandatory_fields = {
            "created_at": operation.created_at,
            "id": operation.id,
            "name": operation.name,
        }
        optional_fields = {
            # Operation run_id and type are not null while operation is created, but may be empty in later events
            "run_id": run_id,
            "type": OperationType(operation.type) if operation.type else None,
            "status": Status(operation.status) if operation.status else None,
            "started_at": operation.started_at,
            "ended_at": operation.ended_at,
            "description": operation.description,
            "position": operation.position,
        }

        insert_statement = insert(Operation)
        insert_statement = insert_statement.on_conflict_do_update(
            index_elements=[Operation.created_at, Operation.id],
            set_={
                # Different operationEvents may have different parts of information - one only 'started_at', second only 'ended_at',
                # third only 'parent_operation_id'. If field value is unknown, we should keep original value
                getattr(Operation, column): getattr(insert_statement.excluded, column)
                for column, value in optional_fields.items()
                if value is not None
            },
        )
        return await self._session.scalar(  # type: ignore[return-value]
            insert_statement.returning(Operation),
            {
                **mandatory_fields,
                **optional_fields,
            },
            execution_options={"populate_existing": True},
        )
