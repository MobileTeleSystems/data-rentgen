from data_rentgen.db.models import Input, Output


async def relation_stats(relations: list[Input] | list[Output]):
    stats = {}
    for relation in relations:
        if relation.dataset_id not in stats:
            stats[relation.dataset_id] = {"num_bytes": 0, "num_rows": 0, "num_files": 0, "created_at": None}

        stats[relation.dataset_id]["num_bytes"] += relation.num_bytes
        stats[relation.dataset_id]["num_rows"] += relation.num_rows
        stats[relation.dataset_id]["num_files"] += relation.num_files
        stats[relation.dataset_id]["created_at"] = max(
            stats[relation.dataset_id]["created_at"] or relation.created_at,
            relation.created_at,
        )

    return stats


async def relation_stats_by_operations(relations: list[Input] | list[Output]):
    stats = {}
    for relation in relations:
        key = (str(relation.operation_id), relation.dataset_id)
        if key not in stats:
            stats[key] = {"num_bytes": 0, "num_rows": 0, "num_files": 0, "created_at": None}

        stats[key]["num_bytes"] += relation.num_bytes
        stats[key]["num_rows"] += relation.num_rows
        stats[key]["num_files"] += relation.num_files
        stats[key]["created_at"] = max(stats[key]["created_at"] or relation.created_at, relation.created_at)

    return stats


async def relation_stats_by_runs(relations: list[Input] | list[Output]):
    stats = {}
    for relation in relations:
        key = (str(relation.run_id), relation.dataset_id)
        if key not in stats:
            stats[key] = {"num_bytes": 0, "num_rows": 0, "num_files": 0, "created_at": None}

        stats[key]["num_bytes"] += relation.num_bytes
        stats[key]["num_rows"] += relation.num_rows
        stats[key]["num_files"] += relation.num_files
        stats[key]["created_at"] = max(stats[key]["created_at"] or relation.created_at, relation.created_at)

    return stats


async def relation_stats_by_jobs(relations: list[Input] | list[Output]):
    stats = {}
    for relation in relations:
        key = (str(relation.job_id), relation.dataset_id)
        if key not in stats:
            stats[key] = {"num_bytes": 0, "num_rows": 0, "num_files": 0, "created_at": None}

        stats[key]["num_bytes"] += relation.num_bytes
        stats[key]["num_rows"] += relation.num_rows
        stats[key]["num_files"] += relation.num_files
        stats[key]["created_at"] = max(stats[key]["created_at"] or relation.created_at, relation.created_at)

    return stats
