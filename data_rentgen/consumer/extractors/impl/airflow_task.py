# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from urllib.parse import quote

from packaging.version import Version

from data_rentgen.consumer.extractors.generic import GenericExtractor
from data_rentgen.dto import OperationDTO, RunDTO, RunStartReasonDTO, UserDTO
from data_rentgen.openlineage.run_event import OpenLineageRunEvent
from data_rentgen.openlineage.run_facets import (
    OpenLineageAirflowDagRunType,
    OpenLineageAirflowTaskRunFacet,
)


class AirflowTaskExtractor(GenericExtractor):
    def match(self, event: OpenLineageRunEvent) -> bool:
        return bool(
            event.job.facets.jobType
            and event.job.facets.jobType.integration == "AIRFLOW"
            and event.job.facets.jobType.jobType == "TASK",
        )

    def is_operation(self, event: OpenLineageRunEvent) -> bool:
        # COMPLETE event could contain inputs or outputs even if START was empty. So Task is always Run + Operation
        return True

    def extract_run(self, event: OpenLineageRunEvent) -> RunDTO:
        run = super().extract_run(event)
        self._enrich_run_identifiers(run, event)
        self._enrich_run_user(run, event)
        self._enrich_run_start_reason(run, event)
        self._enrich_run_log_url(run, event)
        return run

    def _extract_operation(self, event: OpenLineageRunEvent, run: RunDTO) -> OperationDTO:
        operation = super()._extract_operation(event, run)

        airflow_operator_details = event.run.facets.airflow
        if airflow_operator_details:
            operation.name = airflow_operator_details.task.task_id
            operation.description = airflow_operator_details.task.operator_class
            operation.position = airflow_operator_details.taskInstance.map_index
            if airflow_operator_details.task.task_group:
                operation.group = airflow_operator_details.task.task_group.group_id
        else:
            # for FINISHED/KILLED event we don't receive task facet.
            # keep existing name in DB instead of resetting it to "dag.task"
            operation.name = None
        return operation

    def _enrich_run_identifiers(self, run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
        task_run_facet = event.run.facets.airflow
        if task_run_facet:
            run.external_id = task_run_facet.dagRun.run_id
            run.attempt = str(task_run_facet.taskInstance.try_number)
        return run

    def _enrich_run_user(self, run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
        task_run_facet = event.run.facets.airflow
        if task_run_facet and task_run_facet.dag.owner not in (None, "airflow", "***"):
            run.user = UserDTO(name=task_run_facet.dag.owner)  # type: ignore[arg-type]
        return run

    def _enrich_run_start_reason(self, run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
        task_run_facet = event.run.facets.airflow
        if not task_run_facet:
            return run

        if task_run_facet.dagRun.run_type == OpenLineageAirflowDagRunType.MANUAL:
            run.start_reason = RunStartReasonDTO.MANUAL
        else:
            run.start_reason = RunStartReasonDTO.AUTOMATIC
        return run

    def _enrich_run_log_url(self, run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
        task_run_facet = event.run.facets.airflow
        if not task_run_facet:
            return run

        # https://github.com/OpenLineage/OpenLineage/pull/2852
        if task_run_facet.taskInstance.log_url:
            run.persistent_log_url = task_run_facet.taskInstance.log_url
            return run

        airflow_base_url = event.job.namespace
        if not airflow_base_url.startswith("http"):
            return run

        processing_engine = event.run.facets.processing_engine
        if processing_engine and processing_engine.version >= Version("3.0.0"):
            run.persistent_log_url = self._get_airflow_3_x_task_log_url(
                airflow_base_url,
                task_run_facet,
            )
        elif processing_engine and processing_engine.version >= Version("2.9.1"):
            run.persistent_log_url = self._get_airflow_2_9_plus_task_log_url(
                airflow_base_url,
                task_run_facet,
            )
        else:
            run.persistent_log_url = self._get_airflow_2_x_task_log_url(
                airflow_base_url,
                task_run_facet,
            )

        return run

    def _get_airflow_3_x_task_log_url(
        self,
        namespace: str,
        task_run_facet: OpenLineageAirflowTaskRunFacet,
    ) -> str:
        # https://github.com/apache/airflow/pull/48996
        # https://github.com/apache/airflow/blob/3.0.1/airflow-core/src/airflow/models/taskinstance.py#L980-L987
        dag_id = task_run_facet.dag.dag_id
        dag_run_id = task_run_facet.dagRun.run_id
        task_id = task_run_facet.task.task_id
        map_index = task_run_facet.taskInstance.map_index
        try_number = task_run_facet.taskInstance.try_number

        map_index_part = f"/mapped/{map_index}" if map_index is not None else ""
        try_number_part = f"?try_number={try_number}" if try_number > 0 else ""

        return f"{namespace}/dags/{dag_id}/runs/{dag_run_id}/tasks/{task_id}{map_index_part}{try_number_part}"

    def _get_airflow_2_9_plus_task_log_url(
        self,
        namespace: str,
        task_run_facet: OpenLineageAirflowTaskRunFacet,
    ) -> str:
        # https://github.com/apache/airflow/pull/39183
        # https://github.com/apache/airflow/blob/2.9.1/airflow/models/taskinstance.py#L1720-L1734
        dag_id = task_run_facet.dag.dag_id
        dag_run_id = quote(task_run_facet.dagRun.run_id)
        task_id = quote(task_run_facet.task.task_id)
        map_index = task_run_facet.taskInstance.map_index
        if map_index is None:
            map_index = -1
        return (
            f"{namespace}/dags/{dag_id}/grid?tab=logs&dag_run_id={dag_run_id}&task_id={task_id}&map_index={map_index}"
        )

    def _get_airflow_2_x_task_log_url(
        self,
        namespace: str,
        task_run_facet: OpenLineageAirflowTaskRunFacet,
    ) -> str:
        # https://github.com/apache/airflow/blob/2.1.0/airflow/models/taskinstance.py#L524-L528
        dag_id = quote(task_run_facet.dag.dag_id)
        execution_date = quote(
            task_run_facet.dagRun.data_interval_start.isoformat(),
        )
        task_id = quote(task_run_facet.task.task_id)
        return f"{namespace}/log?&dag_id={dag_id}&task_id={task_id}&execution_date={execution_date}"
