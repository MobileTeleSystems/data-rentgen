<a id="architecture"></a>

# Architecture

## Components

Data.Rentgen is build using following components:

* [OpenLineage](https://openlineage.io/docs/) clients & integrations with third-party modules (e.g. Apache Spark, Apache Airflow).
* [Message Broker](broker/index.md#message-broker), receiving events in JSON format.
* [Message Consumer](consumer/index.md#message-consumer), parsing JSON messages.
* [Relation Database](database/index.md#database) for storing consumed & cleaned up data.
* [REST API Server](server/index.md#server), serving database data.
* [Frontend](frontend/index.md#frontend), accessing REST API to navigate created entities & lineage graph.
* [HTTP2Kafka proxy](http2kafka/index.md#http2kafka) (optional), proxy for sending OpenLineage events to Kafka using HTTP API.

## Architecture diagram
