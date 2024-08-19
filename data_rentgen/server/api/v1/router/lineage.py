# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends

from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas import InvalidRequestSchema
from data_rentgen.server.schemas.v1 import LineageQueryV1, LineageResponseV1
from data_rentgen.server.services import LineageService

router = APIRouter(prefix="/lineage", tags=["Lineage"], responses=get_error_responses(include={InvalidRequestSchema}))


@router.get("", summary="Lineage graph")
async def get_lineage(
    pagination_args: Annotated[LineageQueryV1, Depends()],
    lineage_service: Annotated[LineageService, Depends()],
) -> LineageResponseV1:
    return await lineage_service.get_lineage(**pagination_args.model_dump())
