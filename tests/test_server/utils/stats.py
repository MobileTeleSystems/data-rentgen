from data_rentgen.db.models import Input, Output


async def relation_stats(relations: list[Input] | list[Output]):
    dataset_ids = {relation.dataset_id for relation in relations}
    stats = {}
    for dataset_id in dataset_ids:
        stats[dataset_id] = {
            "num_bytes": sum([relation.num_bytes for relation in relations if relation.dataset_id == dataset_id]),
            "num_rows": sum([relation.num_rows for relation in relations if relation.dataset_id == dataset_id]),
            "num_files": sum([relation.num_files for relation in relations if relation.dataset_id == dataset_id]),
            "created_at": max([relation.created_at for relation in relations if relation.dataset_id == dataset_id]),
        }

    return stats


async def relation_stats_by_operations(relations: list[Input] | list[Output]):
    operation_ids = {relation.operation_id for relation in relations}
    stats = {}
    for operation_id in operation_ids:
        dataset_ids = {relation.dataset_id for relation in relations if relation.operation_id == operation_id}
        for dataset_id in dataset_ids:
            stats[(str(operation_id), dataset_id)] = {
                "num_bytes": sum(
                    [
                        relation.num_bytes
                        for relation in relations
                        if relation.operation_id == operation_id and relation.dataset_id == dataset_id
                    ],
                ),
                "num_rows": sum(
                    [
                        relation.num_rows
                        for relation in relations
                        if relation.operation_id == operation_id and relation.dataset_id == dataset_id
                    ],
                ),
                "num_files": sum(
                    [
                        relation.num_files
                        for relation in relations
                        if relation.operation_id == operation_id and relation.dataset_id == dataset_id
                    ],
                ),
                "created_at": max(
                    [
                        relation.created_at
                        for relation in relations
                        if relation.operation_id == operation_id and relation.dataset_id == dataset_id
                    ],
                ),
            }
    return stats


async def relation_stats_by_runs(relations: list[Input] | list[Output]):
    run_ids = {relation.run_id for relation in relations}
    stats = {}
    for run_id in run_ids:
        dataset_ids = {relation.dataset_id for relation in relations if relation.run_id == run_id}
        for dataset_id in dataset_ids:
            stats[(str(run_id), dataset_id)] = {
                "num_bytes": sum(
                    [
                        relation.num_bytes
                        for relation in relations
                        if relation.run_id == run_id and relation.dataset_id == dataset_id
                    ],
                ),
                "num_rows": sum(
                    [
                        relation.num_rows
                        for relation in relations
                        if relation.run_id == run_id and relation.dataset_id == dataset_id
                    ],
                ),
                "num_files": sum(
                    [
                        relation.num_files
                        for relation in relations
                        if relation.run_id == run_id and relation.dataset_id == dataset_id
                    ],
                ),
                "created_at": max(
                    [
                        relation.created_at
                        for relation in relations
                        if relation.run_id == run_id and relation.dataset_id == dataset_id
                    ],
                ),
            }
    return stats


async def relation_stats_by_jobs(relations: list[Input] | list[Output]):
    job_ids = {relation.job_id for relation in relations}
    stats = {}
    for job_id in job_ids:
        dataset_ids = {relation.dataset_id for relation in relations if relation.job_id == job_id}
        for dataset_id in dataset_ids:
            stats[(str(job_id), dataset_id)] = {
                "num_bytes": sum(
                    [
                        relation.num_bytes
                        for relation in relations
                        if relation.job_id == job_id and relation.dataset_id == dataset_id
                    ],
                ),
                "num_rows": sum(
                    [
                        relation.num_rows
                        for relation in relations
                        if relation.job_id == job_id and relation.dataset_id == dataset_id
                    ],
                ),
                "num_files": sum(
                    [
                        relation.num_files
                        for relation in relations
                        if relation.job_id == job_id and relation.dataset_id == dataset_id
                    ],
                ),
                "created_at": max(
                    [
                        relation.created_at
                        for relation in relations
                        if relation.job_id == job_id and relation.dataset_id == dataset_id
                    ],
                ),
            }
    return stats
