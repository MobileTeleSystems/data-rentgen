<a id="overview-setup-airflow"></a>

# Apache Airflow integration

Using [OpenLineage integration with Apache Airflow](https://openlineage.io/docs/integrations/airflow/).

## Requirements

* [Apache Airflow](https://airflow.apache.org/) 2.x or 3.x
* OpenLineage 1.19.0 or higher, recommended 1.34.0+
* OpenLineage integration for Airflow (see below)

## Entity mapping

* Airflow DAG → Data.Rentgen Job
* Airflow DAGRun → Data.Rentgen Run
* Airflow Task → Data.Rentgen Job
* Airflow TaskInstance → Data.Rentgen Run + Data.Rentgen Operation

## Install

* For Airflow 2.7 or higher, use [apache-airflow-providers-openlineage](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/index.html) 1.9.0 or higher:
  ```console
  $ pip install "apache-airflow-providers-openlineage>=2.3.0" "openlineage-python[kafka]>=1.34.0" zstd
  ```
* For Airflow 2.1.x-2.6.x, use [OpenLineage integration for Airflow](https://openlineage.io/docs/integrations/airflow/) 1.19.0 or higher
  ```console
  $ pip install "openlineage-airflow>=1.34.0" "openlineage-python[kafka]>=1.34.0" zstd
  ```

## Setup

### Via OpenLineage config file

* Create `openlineage.yml` file with content like:
  ```yaml
  transport:
      type: kafka
      topic: input.runs
      config:
          bootstrap.servers: localhost:9093
          security.protocol: SASL_PLAINTEXT
          sasl.mechanism: SCRAM-SHA-256
          sasl.username: data_rentgen
          sasl.password: changeme
          compression.type: zstd
          acks: all
  ```
* Pass path to config file via `AIRFLOW__OPENLINEAGE__CONFIG_PATH` environment variable:
  ```ini
  AIRFLOW__OPENLINEAGE__NAMESPACE=http://airflow.hostname.fqdn:8080
  AIRFLOW__OPENLINEAGE__CONFIG_PATH=/path/to/openlineage.yml
  ```

### Via Airflow config file

Setup OpenLineage integration using `airflow.cfg` config file:

```ini
[openlineage]
# set here address of Airflow Web UI
namespace = http://airflow.hostname.fqdn:8080
# set here Kafka connection address & credentials
transport = {"type": "kafka", "config": {"bootstrap.servers": "localhost:9093", "security.protocol": "SASL_PLAINTEXT", "sasl.mechanism": "SCRAM-SHA-256", "sasl.username": "data_rentgen", "sasl.password": "changeme", "compression.type": "zstd", "acks": "all"}, "topic": "input.runs", "flush": true}
```

### Via Airflow environment variables

Set environment variables for all Airflow components (e.g. via `docker-compose.yml`)

```ini
AIRFLOW__OPENLINEAGE__NAMESPACE=http://airflow.hostname.fqdn:8080
AIRFLOW__OPENLINEAGE__TRANSPORT={"type": "kafka", "config": {"bootstrap.servers": "localhost:9093", "security.protocol": "SASL_PLAINTEXT", "sasl.mechanism": "SCRAM-SHA-256", "sasl.username": "data_rentgen", "sasl.password": "changeme", "compression.type": "zstd", "acks": "all"}, "topic": "input.runs", "flush": true}
```

### Airflow 2.1.x and 2.2.x

For Airflow 2.1-2.2 it OpenLineage integration should be enabled explicitly by adding `airflow.cfg` config entry:

```ini
[lineage]
backend=openlineage.lineage_backend.OpenLineageBackend
```

Or by setting up environment variable:

```ini
AIRFLOW__LINEAGE__BACKEND=openlineage.lineage_backend.OpenLineageBackend
```

## Collect and send lineage

Run some Airflow dag with tasks, and wait until finished.
Lineage will be send to Data.Rentgen automatically by OpenLineage integration.

## See results

Browse frontend page [Jobs](http://localhost:3000/jobs) to see what information was extracted by OpenLineage & DataRentgen.

### Job list page

![image](integrations/airflow/job_list.png)

### DAG run details page

![image](integrations/airflow/dag_run_details.png)

### Task run details page

![image](integrations/airflow/task_run_details.png)
