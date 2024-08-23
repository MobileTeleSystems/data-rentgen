from random import choice, randint

from data_rentgen.db.models import Interaction, InteractionType
from data_rentgen.db.utils.uuid import extract_timestamp_from_uuid, generate_new_uuid


def interaction_factory(**kwargs) -> Interaction:
    created_at = kwargs.pop("created_at", None)
    interaction_id = generate_new_uuid(created_at)
    data = {
        "id": interaction_id,
        "created_at": extract_timestamp_from_uuid(interaction_id),
        "operation_id": generate_new_uuid(),
        "dataset_id": randint(0, 10000000),
        "type": choice(list(InteractionType)),
        "schema_id": randint(0, 10000000),
        "num_bytes": randint(0, 10000000),
        "num_rows": randint(0, 10000),
        "num_files": randint(0, 100),
    }
    data.update(kwargs)
    return Interaction(**data)
