import pytest

from data_rentgen.dto import (
    DatasetDTO,
    DatasetSymlinkDTO,
    DatasetSymlinkTypeDTO,
    LocationDTO,
    SchemaDTO,
    UserDTO,
)


@pytest.fixture
def extracted_postgres_location() -> LocationDTO:
    return LocationDTO(
        type="postgres",
        name="192.168.1.1:5432",
        addresses={"postgres://192.168.1.1:5432"},
    )


@pytest.fixture
def extracted_postgres_dataset(
    extracted_postgres_location: LocationDTO,
) -> DatasetDTO:
    return DatasetDTO(
        location=extracted_postgres_location,
        name="mydb.myschema.mytable",
    )


@pytest.fixture
def extracted_hdfs_location() -> LocationDTO:
    return LocationDTO(
        type="hdfs",
        name="test-hadoop:9820",
        addresses={"hdfs://test-hadoop:9820"},
    )


@pytest.fixture
def extracted_hdfs_dataset(
    extracted_hdfs_location: LocationDTO,
) -> DatasetDTO:
    return DatasetDTO(
        location=extracted_hdfs_location,
        name="/user/hive/warehouse/mydb.db/mytable",
    )


@pytest.fixture
def extracted_hive_location() -> LocationDTO:
    return LocationDTO(
        type="hive",
        name="test-hadoop:9083",
        addresses={"hive://test-hadoop:9083"},
    )


@pytest.fixture
def extracted_hive_dataset(
    extracted_hive_location: LocationDTO,
) -> DatasetDTO:
    return DatasetDTO(
        location=extracted_hive_location,
        name="mydb.mytable",
    )


@pytest.fixture
def extracted_hdfs_dataset_symlink(
    extracted_hdfs_dataset: DatasetDTO,
    extracted_hive_dataset: DatasetDTO,
) -> DatasetSymlinkDTO:
    return DatasetSymlinkDTO(
        from_dataset=extracted_hdfs_dataset,
        to_dataset=extracted_hive_dataset,
        type=DatasetSymlinkTypeDTO.METASTORE,
    )


@pytest.fixture
def extracted_hive_dataset_symlink(
    extracted_hdfs_dataset: DatasetDTO,
    extracted_hive_dataset: DatasetDTO,
) -> DatasetSymlinkDTO:
    return DatasetSymlinkDTO(
        from_dataset=extracted_hive_dataset,
        to_dataset=extracted_hdfs_dataset,
        type=DatasetSymlinkTypeDTO.WAREHOUSE,
    )


@pytest.fixture
def extracted_kafka_location() -> LocationDTO:
    return LocationDTO(
        type="kafka",
        name="server1:9092",
        addresses={"kafka://server1:9092", "kafka://server2:9092"},
    )


@pytest.fixture
def extracted_kafka_dataset(
    extracted_kafka_location: LocationDTO,
) -> DatasetDTO:
    return DatasetDTO(
        location=extracted_kafka_location,
        name="mytopic",
    )


@pytest.fixture
def extracted_dataset_schema() -> SchemaDTO:
    return SchemaDTO(
        fields=[
            {
                "name": "dt",
                "type": "timestamp",
                "description": "Business date",
            },
            {
                "name": "customer_id",
                "type": "decimal(20,0)",
            },
            {
                "name": "total_spent",
                "type": "float",
            },
            {
                "name": "phones",
                "type": "array",
                "fields": [
                    {
                        "name": "_element",
                        "type": "string",
                    },
                ],
            },
            {
                "name": "address",
                "type": "struct",
                "fields": [
                    {
                        "name": "street",
                        "type": "string",
                    },
                    {
                        "name": "city",
                        "type": "string",
                    },
                    {
                        "name": "state",
                        "type": "string",
                    },
                    {
                        "name": "zip",
                        "type": "string",
                    },
                ],
            },
        ],
    )


@pytest.fixture
def extracted_user() -> UserDTO:
    return UserDTO(name="myuser")
