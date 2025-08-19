# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from fastapi import APIRouter

from data_rentgen.server.api.v1.router.auth import router as auth_router
from data_rentgen.server.api.v1.router.dataset import router as dataset_router
from data_rentgen.server.api.v1.router.job import router as job_router
from data_rentgen.server.api.v1.router.location import router as location_router
from data_rentgen.server.api.v1.router.operation import router as operation_router
from data_rentgen.server.api.v1.router.personal_token import router as personal_token_router
from data_rentgen.server.api.v1.router.run import router as run_router
from data_rentgen.server.api.v1.router.tag import router as tag_router
from data_rentgen.server.api.v1.router.user import router as user_router

router = APIRouter(prefix="/v1")
router.include_router(auth_router)
router.include_router(dataset_router)
router.include_router(job_router)
router.include_router(location_router)
router.include_router(operation_router)
router.include_router(run_router)
router.include_router(user_router)
router.include_router(personal_token_router)
router.include_router(tag_router)
