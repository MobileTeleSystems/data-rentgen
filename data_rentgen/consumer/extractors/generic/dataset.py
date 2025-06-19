# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from urllib.parse import urlparse

from data_rentgen.consumer.openlineage.dataset import (
    OpenLineageDataset,
)
from data_rentgen.consumer.openlineage.dataset_facets import (
    OpenLineageColumnLineageDatasetFacetFieldRef,
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


class DatasetExtractorMixin:
    def extract_dataset(self, dataset: OpenLineageDataset) -> DatasetDTO:
        """
        Extract DatasetDTO from input or output OpenLineageDataset
        """
        dataset_dto = self._extract_dataset_ref(dataset)
        dataset_dto.format = self._extract_dataset_format(dataset)
        return dataset_dto

    def _extract_dataset_ref(
        self,
        dataset: OpenLineageDataset | OpenLineageColumnLineageDatasetFacetFieldRef | OpenLineageSymlinkIdentifier,
    ) -> DatasetDTO:
        return DatasetDTO(
            name=dataset.name,
            location=self._extract_dataset_location(dataset),
        )

    def _extract_dataset_location(
        self,
        dataset: OpenLineageDataset | OpenLineageSymlinkIdentifier | OpenLineageColumnLineageDatasetFacetFieldRef,
    ) -> LocationDTO:
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

    def _extract_dataset_format(self, dataset: OpenLineageDataset) -> str | None:
        match dataset.facets.storage:
            case OpenLineageStorageDatasetFacet(storageLayer="default", fileFormat=file_format):
                # See https://github.com/OpenLineage/OpenLineage/issues/2770
                return file_format
            case OpenLineageStorageDatasetFacet(storageLayer=storage_layer):
                return storage_layer
            case _:
                return None

    def extract_dataset_and_symlinks(self, dataset: OpenLineageDataset) -> tuple[DatasetDTO, list[DatasetSymlinkDTO]]:
        symlink_identifiers = dataset.facets.symlinks.identifiers if dataset.facets.symlinks else []
        return self._extract_dataset_and_symlinks(dataset, symlink_identifiers)

    def _extract_dataset_and_symlinks(
        self,
        dataset: OpenLineageDataset,
        symlink_identifiers: list[OpenLineageSymlinkIdentifier],
    ) -> tuple[DatasetDTO, list[DatasetSymlinkDTO]]:
        dataset_dto = self.extract_dataset(dataset)
        symlinks = []
        for symlink_identifier in symlink_identifiers:
            symlink_dto = self._extract_dataset_ref(symlink_identifier)
            symlinks.extend(
                self._connect_dataset_with_symlinks(
                    dataset_dto,
                    symlink_dto,
                    symlink_identifier.type,
                ),
            )
        return dataset_dto, symlinks

    def _connect_dataset_with_symlinks(
        self,
        dataset: DatasetDTO,
        symlink: DatasetDTO,
        type_: OpenLineageSymlinkType,
    ) -> list[DatasetSymlinkDTO]:
        result = []
        is_metastore_symlink = type_ == OpenLineageSymlinkType.TABLE

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
