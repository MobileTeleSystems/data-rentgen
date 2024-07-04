# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from faststream import Logger
from faststream.kafka import KafkaRouter

router = KafkaRouter()


@router.subscriber("input")
@router.publisher("output")
async def base_handler(body: dict, logger: Logger):
    logger.info("Test handler, %s", body)
    return {"handler": body}
