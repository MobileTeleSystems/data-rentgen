# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

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

METASTORE = DatasetSymlinkTypeDTO.METASTORE
WAREHOUSE = DatasetSymlinkTypeDTO.WAREHOUSE


def extract_dataset_symlinks(dataset: OpenLineageDataset) -> list[DatasetSymlinkDTO]:
    result: list[DatasetSymlinkDTO] = []
    if not dataset.facets.symlinks:
        return result

    current_dataset = extract_dataset(dataset)
    for identifier in dataset.facets.symlinks.identifiers:
        symlink = extract_dataset(identifier)
        is_metastore_symlink = identifier.type == OpenLineageSymlinkType.TABLE

        result.append(
            DatasetSymlinkDTO(
                from_dataset=current_dataset,
                to_dataset=symlink,
                type=METASTORE if is_metastore_symlink else WAREHOUSE,
            ),
        )
        result.append(
            DatasetSymlinkDTO(
                from_dataset=symlink,
                to_dataset=current_dataset,
                type=WAREHOUSE if is_metastore_symlink else METASTORE,
            ),
        )

    return result


def extract_datasets(dataset: OpenLineageDataset) -> list[DatasetDTO]:
    result = [extract_dataset(dataset)]

    if dataset.facets.symlinks:
        for identifier in dataset.facets.symlinks.identifiers:
            result.append(extract_dataset(identifier))
    return result


def extract_dataset(dataset: OpenLineageDataset | OpenLineageSymlinkIdentifier) -> DatasetDTO:
    return DatasetDTO(
        name=dataset.name,
        location=extract_dataset_location(dataset),
        format=extract_dataset_format(dataset),
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
        addresses=[f"{scheme}://{host}" for host in hosts],
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
