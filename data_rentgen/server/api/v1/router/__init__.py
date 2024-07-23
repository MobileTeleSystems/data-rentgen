# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from fastapi import APIRouter

from data_rentgen.server.api.v1.router.dataset import router as dataset_router
from data_rentgen.server.api.v1.router.job import router as job_router
from data_rentgen.server.api.v1.router.run import router as run_router

router = APIRouter(prefix="/v1")
router.include_router(job_router)
router.include_router(dataset_router)
router.include_router(run_router)
