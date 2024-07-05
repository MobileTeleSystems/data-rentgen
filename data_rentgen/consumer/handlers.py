# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from faststream import Logger
from faststream.kafka import KafkaRouter

from data_rentgen.consumer.openlineage.run_event import OpenLineageRunEvent

router = KafkaRouter()


@router.subscriber("input.runs")
async def runs_handler(msg: OpenLineageRunEvent, logger: Logger):
    logger.info("Successfully handled, %s", msg)
