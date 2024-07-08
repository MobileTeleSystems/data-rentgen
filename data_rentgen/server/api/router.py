# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from fastapi import APIRouter

from data_rentgen.server.api.monitoring import router as monitoring_router

api_router = APIRouter()
api_router.include_router(monitoring_router)