from dataclasses import dataclass, field

from data_rentgen.db.models import (
    Dataset,
    DatasetSymlink,
    Input,
    Job,
    Operation,
    Output,
    Run,
)


@dataclass
class LineageResult:
    jobs: list[Job] = field(default_factory=list)
    runs: list[Run] = field(default_factory=list)
    operations: list[Operation] = field(default_factory=list)
    datasets: list[Dataset] = field(default_factory=list)
    inputs: list[Input] = field(default_factory=list)
    outputs: list[Output] = field(default_factory=list)
    dataset_symlinks: list[DatasetSymlink] = field(default_factory=list)
