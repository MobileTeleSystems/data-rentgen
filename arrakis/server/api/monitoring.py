# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from fastapi import APIRouter

from arrakis.server.schemas import PingResponse

router = APIRouter(tags=["Monitoring"], prefix="/monitoring")


@router.get("/ping", summary="Check if server is alive")
async def ping() -> PingResponse:
    return PingResponse()
