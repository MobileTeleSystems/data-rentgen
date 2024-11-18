from data_rentgen.db.models import Input, Output


def relation_stats_by_runs(relations: list[Input] | list[Output]):
    stats = {}
    for relation in relations:
        key = (relation.run_id, relation.dataset_id)
        if key not in stats:
            stats[key] = {"num_bytes": 0, "num_rows": 0, "num_files": 0, "created_at": None}

        stats[key]["num_bytes"] += relation.num_bytes
        stats[key]["num_rows"] += relation.num_rows
        stats[key]["num_files"] += relation.num_files
        stats[key]["created_at"] = max(stats[key]["created_at"] or relation.created_at, relation.created_at)

    return stats


def relation_stats_by_jobs(relations: list[Input] | list[Output]):
    stats = {}
    for relation in relations:
        key = (relation.job_id, relation.dataset_id)
        if key not in stats:
            stats[key] = {"num_bytes": 0, "num_rows": 0, "num_files": 0, "created_at": None}

        stats[key]["num_bytes"] += relation.num_bytes
        stats[key]["num_rows"] += relation.num_rows
        stats[key]["num_files"] += relation.num_files
        stats[key]["created_at"] = max(stats[key]["created_at"] or relation.created_at, relation.created_at)

    return stats
