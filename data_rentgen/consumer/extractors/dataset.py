# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import logging
from urllib.parse import urlparse

from data_rentgen.consumer.openlineage.dataset import OpenLineageDataset
from data_rentgen.consumer.openlineage.dataset_facets import (
    OpenLineageStorageDatasetFacet,
    OpenLineageSymlinkIdentifier,
    OpenLineageSymlinkType,
)
from data_rentgen.dto import (
    DatasetDTO,
    DatasetSymlinkDTO,
    DatasetSymlinkTypeDTO,
    LocationDTO,
)

logger = logging.getLogger(__name__)

METASTORE = DatasetSymlinkTypeDTO.METASTORE
WAREHOUSE = DatasetSymlinkTypeDTO.WAREHOUSE


def connect_dataset_with_symlinks(
    dataset: DatasetDTO,
    symlink: DatasetDTO,
    type: OpenLineageSymlinkType,
) -> list[DatasetSymlinkDTO]:
    result = []
    is_metastore_symlink = type == OpenLineageSymlinkType.TABLE

    result.append(
        DatasetSymlinkDTO(
            from_dataset=dataset,
            to_dataset=symlink,
            type=METASTORE if is_metastore_symlink else WAREHOUSE,
        ),
    )
    result.append(
        DatasetSymlinkDTO(
            from_dataset=symlink,
            to_dataset=dataset,
            type=WAREHOUSE if is_metastore_symlink else METASTORE,
        ),
    )

    return sorted(result, key=lambda x: x.type)


def extract_dataset(dataset: OpenLineageDataset | OpenLineageSymlinkIdentifier) -> DatasetDTO:
    return DatasetDTO(
        name=dataset.name,
        location=extract_dataset_location(dataset),
        format=extract_dataset_format(dataset),
    )


def extract_dataset_and_symlinks(dataset: OpenLineageDataset) -> tuple[DatasetDTO, list[DatasetSymlinkDTO]]:
    if not dataset.facets.symlinks:
        return extract_dataset(dataset), []

    table_symlinks = [
        identifier
        for identifier in dataset.facets.symlinks.identifiers
        if identifier.type == OpenLineageSymlinkType.TABLE
    ]
    if table_symlinks:
        # We are swapping the dataset with its TABLE symlink to create a cleaner lineage.
        # For example, by replacing an HDFS file with its corresponding Hive table.
        # This ensures that all operations interact with a single table instead of multiple files (which may represent different partitions).
        # Discussion on this issue: https://github.com/OpenLineage/OpenLineage/issues/2718

        # TODO: add support for multiple TABLE symlinks
        if len(table_symlinks) > 1:
            logger.warning(
                "Dataset has more than one TABLE symlink. Only the first one will be used for replacement. Symlink name: %s",
                table_symlinks[0].name,
            )
            table_dataset = table_symlinks[0]
            return (
                DatasetDTO(
                    name=table_dataset.name,
                    location=extract_dataset_location(table_dataset),
                    format=extract_dataset_format(table_dataset),
                ),
                connect_dataset_with_symlinks(
                    extract_dataset(dataset),
                    extract_dataset(table_dataset),
                    OpenLineageSymlinkType.TABLE,
                ),
            )

    symlinks = []
    for identifier in dataset.facets.symlinks.identifiers:
        symlinks.extend(
            connect_dataset_with_symlinks(
                extract_dataset(dataset),
                extract_dataset(identifier),
                identifier.type,
            ),
        )

    return (
        DatasetDTO(
            name=dataset.name,
            location=extract_dataset_location(dataset),
            format=extract_dataset_format(dataset),
        ),
        symlinks,
    )


def extract_dataset_location(dataset: OpenLineageDataset | OpenLineageSymlinkIdentifier) -> LocationDTO:
    namespace = dataset.namespace
    if namespace == "file":
        # TODO: remove after https://github.com/OpenLineage/OpenLineage/issues/2709
        namespace = "file://"

    url = urlparse(namespace)
    scheme = url.scheme or "unknown"

    # TODO: handle S3 bucket properly after https://github.com/OpenLineage/OpenLineage/issues/2816
    netloc = url.netloc or url.path
    hosts = list(filter(None, netloc.split(","))) or ["unknown"]
    return LocationDTO(
        type=scheme,
        name=hosts[0],
        addresses={f"{scheme}://{host}" for host in hosts},
    )


def extract_dataset_format(dataset: OpenLineageDataset | OpenLineageSymlinkIdentifier) -> str | None:
    if isinstance(dataset, OpenLineageSymlinkIdentifier):
        return None

    match dataset.facets.storage:
        case OpenLineageStorageDatasetFacet(storageLayer="default", fileFormat=file_format):
            # See https://github.com/OpenLineage/OpenLineage/issues/2770
            return file_format
        case OpenLineageStorageDatasetFacet(storageLayer=storage_layer):
            return storage_layer
        case _:
            return None
