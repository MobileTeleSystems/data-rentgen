from collections.abc import Callable, Sequence
from typing import Any, TypeVar

from data_rentgen.db.models import Input, Output
from data_rentgen.db.repositories.input import InputRow
from data_rentgen.db.repositories.output import OutputRow

IO = TypeVar("IO", Input, Output)


def _not_none(value: Any) -> bool:
    return value is not None


def merge_io(inputs_outputs: Sequence[IO], get_key: Callable[[IO], tuple]) -> list[IO]:
    if not inputs_outputs:
        return []
    io_map = {
        "Input": InputRow,
        "Output": OutputRow,
    }
    io_type = io_map.get(type(inputs_outputs[0]).__name__)
    merged_inputs_outputs = {}
    for raw_io in inputs_outputs:
        key = get_key(raw_io)
        merged_io = merged_inputs_outputs.get(key)

        if not merged_io:
            merged_io = io_type(
                operation_id=raw_io.operation_id,
                run_id=raw_io.run_id,
                job_id=raw_io.job_id,
                dataset_id=raw_io.dataset_id,
                created_at=raw_io.created_at,
                num_bytes=raw_io.num_bytes,
                num_files=raw_io.num_files,
                num_rows=raw_io.num_rows,
                schema_id=raw_io.schema_id,
                schema=raw_io.schema,
                schema_relevance_type="EXACT_MATCH" if raw_io.schema else None,
            )
            if isinstance(merged_io, OutputRow):
                merged_io.types_combined = raw_io.type

            merged_inputs_outputs[key] = merged_io
        else:
            merged_io.created_at = max(merged_io.created_at, raw_io.created_at)
            merged_io.num_bytes = sum(filter(_not_none, [merged_io.num_bytes, raw_io.num_bytes]))
            merged_io.num_files = sum(filter(_not_none, [merged_io.num_files, raw_io.num_files]))
            merged_io.num_rows = sum(filter(_not_none, [merged_io.num_rows, raw_io.num_rows]))

            merged_io.schema_id = merged_io.schema_id or raw_io.schema_id
            if (
                merged_io.schema_id is not None
                and raw_io.schema_id is not None
                and merged_io.schema_id != raw_io.schema_id
            ):
                schema_id = max(merged_io.schema_id, raw_io.schema_id)
                merged_io.schema = merged_io.schema if schema_id == merged_io.schema_id else raw_io.schema
                merged_io.schema_id = schema_id
                merged_io.schema_relevance_type = "LATEST_KNOWN"

            if isinstance(merged_io, OutputRow):
                # cannot merge different types
                merged_io.types_combined |= raw_io.type
    return list(merged_inputs_outputs.values())


def merge_io_by_runs(inputs_outputs: Sequence[IO]) -> list[IO]:
    return merge_io(inputs_outputs, get_key=lambda x: (x.run_id, x.dataset_id))


def merge_io_by_jobs(inputs_outputs: Sequence[IO]) -> list[IO]:
    return merge_io(inputs_outputs, get_key=lambda x: (x.job_id, x.dataset_id))
