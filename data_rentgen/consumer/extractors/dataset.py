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


def connect_dataset_with_symlinks(dataset: DatasetDTO, symlinks: list[DatasetDTO]) -> list[DatasetSymlinkDTO]:
    result: list[DatasetSymlinkDTO] = []

    for identifier in symlinks:
        is_metastore_symlink = identifier.symlink_type == OpenLineageSymlinkType.TABLE

        result.append(
            DatasetSymlinkDTO(
                from_dataset=dataset,
                to_dataset=identifier,
                type=METASTORE if is_metastore_symlink else WAREHOUSE,
            ),
        )
        result.append(
            DatasetSymlinkDTO(
                from_dataset=identifier,
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
        symlink_type=dataset.type if isinstance(dataset, OpenLineageSymlinkIdentifier) else None,
    )


def extract_io_dataset(dataset: OpenLineageDataset) -> DatasetDTO:
    if dataset.facets.symlinks:
        table_symlinks = [
            identifier
            for identifier in dataset.facets.symlinks.identifiers
            if identifier.type == OpenLineageSymlinkType.TABLE
        ]
        if table_symlinks:
            # TODO: add support for multiple TABLE symlinks
            if len(table_symlinks) > 1:
                logger.warning(
                    "Dataset has more than one TABLE symlink. Only the first one will be used for replacement. Symlink name: %s",
                    table_symlinks[0].name,
                )
            table_dataset = table_symlinks[0]
            return DatasetDTO(
                name=table_dataset.name,
                location=extract_dataset_location(table_dataset),
                format=extract_dataset_format(table_dataset),
                dataset_symlinks=[extract_dataset(dataset)],
                symlink_type=table_dataset.type,
            )
        return DatasetDTO(
            name=dataset.name,
            location=extract_dataset_location(dataset),
            format=extract_dataset_format(dataset),
            dataset_symlinks=[extract_dataset(symlink) for symlink in dataset.facets.symlinks.identifiers],
        )
    return extract_dataset(dataset)


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
