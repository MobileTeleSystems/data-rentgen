from datetime import datetime
from typing import Literal

from data_rentgen.db.models import (
    Address,
    Dataset,
    DatasetSymlink,
    Input,
    Job,
    Location,
    Operation,
    Output,
    Run,
    Schema,
    User,
)


def format_datetime(value: datetime):
    result = value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    # pydantic datetime formatter quirk
    return result.replace(".000000", "")


def run_parent_to_json(run: Run):
    return {
        "kind": "PARENT",
        "from": {"kind": "JOB", "id": run.job_id},
        "to": {"kind": "RUN", "id": str(run.id)},
    }


def run_parents_to_json(runs: list[Run]):
    return [run_parent_to_json(run) for run in sorted(runs, key=lambda x: x.id)]


def operation_parent_to_json(operation: Operation):
    return {
        "kind": "PARENT",
        "from": {"kind": "RUN", "id": str(operation.run_id)},
        "to": {"kind": "OPERATION", "id": str(operation.id)},
    }


def operation_parents_to_json(operations: list[Operation]):
    return [operation_parent_to_json(run) for run in sorted(operations, key=lambda x: x.id)]


def symlink_to_json(symlink: DatasetSymlink):
    return {
        "kind": "SYMLINK",
        "from": {"kind": "DATASET", "id": symlink.from_dataset_id},
        "to": {"kind": "DATASET", "id": symlink.to_dataset_id},
        "type": symlink.type.value,
    }


def symlinks_to_json(symlinks: list[DatasetSymlink]):
    return [symlink_to_json(run) for run in sorted(symlinks, key=lambda x: x.id)]


def schema_to_json(schema: Schema):
    return {
        "id": schema.id,
        "fields": [
            {
                "description": None,
                "fields": [],
                **field,
            }
            for field in schema.fields
        ],
    }


def input_to_json(input: Input, granularity: Literal["OPERATION", "RUN", "JOB"]):
    if granularity == "OPERATION":
        to = {"kind": "OPERATION", "id": str(input.operation_id)}
    elif granularity == "RUN":
        to = {"kind": "RUN", "id": str(input.run_id)}
    else:
        to = {"kind": "JOB", "id": input.job_id}

    return {
        "kind": "INPUT",
        "from": {"kind": "DATASET", "id": input.dataset_id},
        "to": to,
        "num_bytes": input.num_bytes,
        "num_rows": input.num_rows,
        "num_files": input.num_files,
        "schema": schema_to_json(input.schema) if input.schema else None,
        "last_interaction_at": format_datetime(input.created_at),
    }


def inputs_to_json(inputs: list[Input], granularity: Literal["OPERATION", "RUN", "JOB"]):
    return [
        input_to_json(input, granularity)
        for input in sorted(inputs, key=lambda x: (x.dataset_id, x.operation_id or x.run_id or x.job_id))
    ]


def output_to_json(output: Output, granularity: Literal["OPERATION", "RUN", "JOB"]):
    if granularity == "OPERATION":
        from_ = {"kind": "OPERATION", "id": str(output.operation_id)}
    elif granularity == "RUN":
        from_ = {"kind": "RUN", "id": str(output.run_id)}
    else:
        from_ = {"kind": "JOB", "id": output.job_id}

    return {
        "kind": "OUTPUT",
        "from": from_,
        "to": {"kind": "DATASET", "id": output.dataset_id},
        "type": output.type.value if output.type else None,
        "num_bytes": output.num_bytes,
        "num_rows": output.num_rows,
        "num_files": output.num_files,
        "schema": schema_to_json(output.schema) if output.schema else None,
        "last_interaction_at": format_datetime(output.created_at),
    }


def outputs_to_json(outputs: list[Output], granularity: Literal["OPERATION", "RUN", "JOB"]):
    return [
        output_to_json(output, granularity)
        for output in sorted(outputs, key=lambda x: (x.operation_id or x.run_id or x.job_id, x.dataset_id))
    ]


def address_to_json(address: Address):
    return {"url": address.url}


def location_to_json(location: Location):
    return {
        "id": location.id,
        "name": location.name,
        "type": location.type,
        "addresses": [address_to_json(address) for address in location.addresses],
        "external_id": location.external_id,
    }


def locations_to_json(locations: list[Location]):
    return [location_to_json(location) for location in sorted(locations, key=lambda x: x.id)]


def dataset_to_json(dataset: Dataset):
    return {
        "kind": "DATASET",
        "id": dataset.id,
        "format": dataset.format,
        "name": dataset.name,
        "location": location_to_json(dataset.location),
    }


def datasets_to_json(datasets: list[Dataset]):
    return [dataset_to_json(dataset) for dataset in sorted(datasets, key=lambda x: x.id)]


def job_to_json(job: Job):
    return {
        "kind": "JOB",
        "id": job.id,
        "name": job.name,
        "type": job.type.value,
        "location": location_to_json(job.location),
    }


def jobs_to_json(jobs: list[Job]):
    return [job_to_json(job) for job in sorted(jobs, key=lambda x: x.id)]


def user_to_json(user: User):
    return {"name": user.name}


def run_to_json(run: Run):
    return {
        "kind": "RUN",
        "id": str(run.id),
        "job_id": run.job_id,
        "created_at": format_datetime(run.created_at),
        "parent_run_id": str(run.parent_run_id),
        "status": run.status.name,
        "external_id": run.external_id,
        "attempt": run.attempt,
        "persistent_log_url": run.persistent_log_url,
        "running_log_url": run.running_log_url,
        "started_at": format_datetime(run.started_at) if run.started_at else None,
        "started_by_user": user_to_json(run.started_by_user) if run.started_by_user else None,
        "start_reason": run.start_reason.value if run.start_reason else None,
        "ended_at": format_datetime(run.ended_at) if run.ended_at else None,
        "end_reason": run.end_reason,
    }


def runs_to_json(runs: list[Run]):
    return [run_to_json(run) for run in sorted(runs, key=lambda x: x.id)]


def operation_to_json(operation: Operation):
    return {
        "kind": "OPERATION",
        "id": str(operation.id),
        "created_at": format_datetime(operation.created_at),
        "run_id": str(operation.run_id),
        "name": operation.name,
        "status": operation.status.name,
        "type": operation.type.value,
        "position": operation.position,
        "group": operation.group,
        "description": operation.description,
        "started_at": format_datetime(operation.started_at) if operation.started_at else None,
        "ended_at": format_datetime(operation.ended_at) if operation.ended_at else None,
    }


def operations_to_json(operations: list[Operation]):
    return [operation_to_json(operation) for operation in sorted(operations, key=lambda x: x.id)]
