from random import choice, randint

from data_rentgen.db.models import Output, OutputType
from data_rentgen.db.utils.uuid import extract_timestamp_from_uuid, generate_new_uuid


def output_factory(**kwargs) -> Output:
    created_at = kwargs.pop("created_at", None)
    output_id = generate_new_uuid(created_at)
    data = {
        "id": output_id,
        "created_at": extract_timestamp_from_uuid(output_id),
        "operation_id": generate_new_uuid(),
        "run_id": generate_new_uuid(),
        "job_id": randint(0, 10000000),
        "dataset_id": randint(0, 10000000),
        "type": choice(list(OutputType)),
        "schema_id": randint(0, 10000000),
        "num_bytes": randint(0, 10000000),
        "num_rows": randint(0, 10000),
        "num_files": randint(0, 100),
    }
    data.update(kwargs)
    return Output(**data)
