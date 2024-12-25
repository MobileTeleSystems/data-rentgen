.. _entities:

Entities
========

.. plantuml::

    @startuml
        title Entities diagram
        skinparam componentStyle rectangle
        left to right direction

        component Location1 {
            collections Addresses1
        }
        component Location2 {
            collections Addresses2
        }
        component Location3 {
            collections Addresses3
        }
        database Dataset1
        database Dataset2
        component Job
        collections Run
        collections Operation
        component User

        [Dataset1] --> [Location1]: located in
        [Dataset2] --> [Location2]: located in
        [Dataset1] --> [Dataset2]: SYMLINK
        [Dataset2] --> [Dataset1]: SYMLINK
        [Job] --> [Location3]: located in
        [Run] --> [Job]: PARENT
        [Operation] --> [Run]: PARENT
        [Run] --> [User]: started by
        [Dataset1] ..> [Operation]: INPUT
        [Operation] ..> [Dataset1]: OUTPUT

    @enduml

Nodes
-----

Nodes are independent entities which describe information about some real entity, like table, ETL job, ETL job run and so on.

Location
~~~~~~~~

Represents information "where is dataset located", "where is job started from".
This is the analog of `OpenLineage namespace <https://openlineage.io/docs/spec/naming/>`_ concept.

Examples:

- ``hive://some-cluster``
- ``hdfs://some-cluster``
- ``oracle://some.host.name:1521``
- ``postgres://some.host.name:5432``
- ``yarn://some-cluster``
- ``local://some.host.name``
- ``http://airflow-web-ui.domain.com:8080``

It contains following fields:

- ``id: int`` - internal unique identifier.
- ``type: str`` - location type, e.g. ``hive``, ``hdfs``, ``oracle`` and so on.
- ``name: str`` - location name, e.g. ``some-cluster``, ``some.host.name``
- ``external_id: str | None`` - external identified of this location in some third-party system (e.g. PlatformInstance in `Datahub <https://datahubproject.io/>`_).
- ``addresses`` - list of alternative location addresses (see below):

  - ``url: str`` - alternative address, in URL form.

Location addresses
^^^^^^^^^^^^^^^^^^

In real world, the same physical host or cluster may have multiple addresses, for example:

- PostgreSQL instance may be accessed by its host name ``postgres://some.host.name:5432`` or by IP ``postgres://192.128.20.14:5432``
- With or without port number - ``postgres://some.host.name:5432`` vs. ``postgres://some.host.name``

Also clusters like Hadoop, Kafka and so on, may be accessed by multiple host names:

- ``hdfs://some-cluster`` → ``[hdfs://some-cluster.name.node1:8082, hdfs://some-cluster.name.node2]``.
- ``kafka://bootstrap.server1,bootstrap.server2,bootstrap.server3`` → ``[kafka://bootstrap.server1,kafka://bootstrap.server2,kafka://bootstrap.server3]``.

Each Spark application run may connect to any of these addresses, and access the same data.

Having a list of alternative addresses of specific location allows to resolve this ambiguity, and always match the same physical table on the same cluster
to the same Data.Rentgen dataset. This prevents creating duplicates of dataset or job.

Dataset
~~~~~~~

Represents information about some table/topic/collection/folder, stored in specific location.

Examples:

- ``hive://some-cluster`` + ``myschema.mytable`` - table inside a Hive cluster.
- ``postgres://some.host.name:5432`` + ``mydb.myschema.mytable`` - table inside a Postgres instance.
- ``hdfs://some-cluster`` + ``/app/warehouse/hive/managed/myschema.db/mytable`` - folder inside a HDFS cluster.

It contains following fields:

- ``id: int`` - internal unique identifier.
- ``location: Location`` - Location where data is actually stored in, like RDMBS instance or cluster.
- ``name: str`` - qualified name of Dataset, like ``mydb.myschema.mytable`` or ``/app/warehouse/hive/managed/myschema.df/mytable``
- ``format: str | None`` - data format used in this dataset, like ``parquet``, ``avro``.

Job
~~~

Represents information about ETL job, run in specific location.
This is an abstraction to group by different runs of the same Spark application or Airflow DAG/Airflow task.

Examples:

- ``yarn://some-cluster`` + ``my-spark-session`` - Spark applicationName, running inside a YARN cluster (``master=yarn``).
- ``local://some.host.name`` + ``my-spark-session`` - Spark applicationName, running on a host (``master=local``).
- ``http://airflow-web-ui.domain.com:8080`` + ``my_dag`` - Airflow DAG, created in Airflow instance.
- ``http://airflow-web-ui.domain.com:8080`` + ``my_dag.mytask`` - Airflow task within Airflow DAG, created in Airflow instance.

It contains following fields:

- ``id: int`` - internal unique identifier.
- ``location: Location`` - Location where Job is run, e.g. cluster or host name.
- ``name: str`` - name of Job, like ``my-session-name``, ``mydag``, ``mydag.mytask``
- ``type: enum`` - type of Job. Currently these types are supported:

  - ``SPARK_APPLICATION``
  - ``AIRLOW_DAG``
  - ``AIRFLOW_TASK``
  - ``UNKNOWN``

User
~~~~

Represents information about some user.

It contains following fields:

- ``id: bigint`` - internal unique identifier.
- ``name: str`` - username.

Run
~~~

Represents information about Job run:

- for Spark applicationName it is a Spark applicationId
- for Airflow DAG it is a DagRun
- for Airflow Task it is a TaskInstance

It contains following fields:

- ``id: uuidv7`` - unique identifier, generated on client.
- ``created_at: timestamp`` - extracted UUIDv7 timestamp, used for filtering purpose.
- ``job_id: int`` - bound to specific Job.
- ``parent_run_id: int`` - parent Run which triggered this specific Run, e.g. Spark applicationId was triggered by Airflow Task Instance.
- ``started_at: timestamp | None`` - timestamp when OpenLineage event with ``eventType=START`` was received.
- ``started_by user: User | None`` - Spark session started as specific OS user/Kerberos principal.
- ``start_reason: Enum | None`` - "why this Run was started?":

  - ``MANUAL``
  - ``AUTOMATIC`` - e.g. by schedule or triggered by another run.

- ``status: Enum`` - run status. Currently these statuses are supported:

  - ``UNKNOWN``
  - ``STARTED``
  - ``SUCCEEDED``
  - ``FAILED``
  - ``KILLED``

- ``ended_at: timestamp | None`` - timestamp when OpenLineage event with ``eventType=COMPLETE|FAIL|ABORT`` was received.
- ``ended_reason: str | None`` - reason of receiving this status, if it is ``FAILED`` or ``KILLED``.
- ``external_id : str | None`` - external identifier of this Run, e.g. Spark ``applicationId`` or Airflow ``dag_run_id``.
- ``attempt: str | None`` - external attempt number of this Run, e.g. Spark ``attemptId`` in YARN, or Airflow Task ``try_number``.
- ``running_log_url: str | None`` - external URL there specific Run information could be found (e.g. Spark UI).
- ``persistent_log_url: str | None`` - external URL there specific Run logs could be found (e.g. Spark History server, Airflow Web UI).


Operation
~~~~~~~~~

Represents specific Spark job or Spark execution information. For now, Airflow DAG and Airflow task does not have any operations.

It contains following fields:

- ``id: uuidv7`` - unique identifier, generated on client.
- ``created_at: timestamp`` - extracted UUIDv7 timestamp, used for filtering purpose.
- ``run_id: uuidv7`` - bound to specific Run.
- ``started_at: timestamp | None`` - timestamp when OpenLineage event with ``eventType=START`` was received.
- ``status: Enum`` - run status. Currently these statuses are supported:

  - ``UNKNOWN``
  - ``STARTED``
  - ``SUCCEEDED``
  - ``FAILED``
  - ``KILLED``

- ``ended_at: timestamp | None`` - timestamp when OpenLineage event with ``eventType=COMPLETE|FAIL|ABORT`` was received.
- ``name: str`` - name of Operation, e.g. Spark command name in ``snake_case``.
- ``position: int | None`` - positional number of Spark job in Spark UI, to simplify matching Data.Rentgen Operation with specific Spark job.
- ``group: str | None`` - Spark job ``jobGroup`` field, to simplify matching Data.Rentgen Operation with specific Spark job.
- ``description: str | None`` - Spark job ``jobDescription`` field, to simplify matching Data.Rentgen Operation with specific Spark job.


Relations
---------

These entities describe relationship between different nodes.

Dataset Symlink
~~~~~~~~~~~~~~~

Represents dataset relations like ``Hive table → HDFS location of table``, and vice versa.

It contains following fields:

- ``from: Dataset`` - symlink starting point.
- ``to: Dataset`` - symlink end point.
- ``type: Enum`` - type of symlink. these types are supported:

  - ``METASTORE`` - from HDFS location to Hive table in metastore.
  - ``WAREHOUSE`` - from Hive table to HDFS/S3 location.

.. note::

    Currently, OpenLineage sends only symlinks ``HDFS location → Hive table`` which `do not exist in the real world <https://github.com/OpenLineage/OpenLineage/issues/2718#issuecomment-2134746258>`_.
    Message consumer automatically adds a reverse symlink ``Hive table → HDFS location`` to simplify building lineage graph, but this is temporary solution.

Parent Relation
~~~~~~~~~~~~~~~

Relation between child run/operation and its parent. For example:

- Spark applicationName is parent for all its runs (applicationId).
- Spark applicationId is parent for all its Spark job or Spark execution.
- Airflow DAG is parent of Airflow task.
- Airflow Task Instance triggered a Spark applicationId.

It contains following fields:

- ``from: Job | Run`` - parent entity.
- ``to: Run | Operation`` - child entity.

Input relation
~~~~~~~~~~~~~~

Relation Dataset → Operation, describing the process of reading some data from specific table/folder by specific Spark operation.

It is also possible to aggregate all inputs of specific Dataset → Run or Dataset → Job combination, by adjusting ``granularity`` option of Lineage graph.

It contains following fields:

- ``from: Dataset`` - data source.
- ``to: Operation | Run | Job`` - data target.
- ``num_rows: int | None`` - number of rows read from dataset. For ``granularity=JOB|RUN`` it is a sum of all read rows from this dataset.
- ``num_bytes: int | None`` - number of bytes read from dataset. For ``granularity=JOB|RUN`` it is a sum of all read bytes from this dataset.
- ``num_files: int | None`` - number of files read from dataset. For ``granularity=JOB|RUN`` it is a sum of all read files from this dataset.

Output relation
~~~~~~~~~~~~~~~

Relation Operation → Dataset, describing the process of writing some data to specific table/folder by specific Spark operation, or table/folder metadata changes.

It is also possible to aggregate all outputs of specific Run → Dataset or Job → Dataset combination, by adjusting ``granularity`` option of Lineage graph.

It contains following fields:

- ``from: Operation | Run | Job`` - output source.
- ``to: Dataset`` - output target.
- ``type: Enum`` - type of output. these types are supported:

  - ``CREATE``
  - ``ALTER``
  - ``RENAME``
  - ``APPEND``
  - ``OVERWRITE``
  - ``DROP``
  - ``TRUNCATE``

- ``num_rows: int | None`` - number of rows written from dataset. For ``granularity=JOB|RUN`` it is a sum of all written rows from this dataset.
- ``num_bytes: int | None`` - number of bytes written from dataset. For ``granularity=JOB|RUN`` it is a sum of all written bytes from this dataset.
- ``num_files: int | None`` - number of files written from dataset. For ``granularity=JOB|RUN`` it is a sum of all written files from this dataset.
