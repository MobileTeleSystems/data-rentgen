# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.openlineage.run_event import OpenLineageRunEvent
from data_rentgen.dto import UserDTO


def extract_run_user(event: OpenLineageRunEvent) -> UserDTO | None:
    spark_application_details = event.run.facets.spark_applicationDetails
    if spark_application_details:
        return UserDTO(name=spark_application_details.userName)
    # Airflow DAG and task have 'owner' field, but if can be either user or group name,
    # and also it does not mean that this exact user started this run.
    return None
