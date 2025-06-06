.. _overview-setup-flink1:

Apache Flink 1.x integration
============================

Using `OpenLineage integration with Apache Flink 1.x <https://openlineage.io/docs/integrations/flink/flink1>`_.

Requirements
------------

* `Apache Flink <https://flink.apache.org/>`_ 1.x
* OpenLineage 1.31.0 or higher, recommended 1.33.0+

Entity mapping
--------------

* Flink job → Data.Rentgen Job
* Flink job run → Data.Rentgen Run + Data.Rentgen Operation

Installation
------------

* Add dependencies `openlineage-flink <https://mvnrepository.com/artifact/io.openlineage/openlineage-java>`_ and `kafka-clients <https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients>`_ to your Flink job:

  .. code-block:: groovy
    :caption: build.gradle

    implementation "io.openlineage:openlineage-flink:1.33.0"
    implementation "org.apache.kafka:kafka-clients:3.9.0"

* Register ``OpenLineageFlinkJobListener`` in the code of your Flink job:

  .. code-block:: java
    :caption: MyFlinkJob.java

    import io.openlineage.flink.OpenLineageFlinkJobListener;

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    JobListener listener = OpenLineageFlinkJobListener.builder()
        .executionEnvironment(env)
        .build();
    env.registerJobListener(listener);

Setup
-----

* Modify ``JobManager`` configuration to include:

  .. code-block:: yaml
    :caption: config.yaml

    execution.attached: true  # capture job stop events

* Create ``openlineage.yml`` file with content like:

  .. code-block:: yaml
    :caption: openlineage.yml

    job:
        namespace: http://some.host.fqdn:18081  # set namespace to match Flink address
        name: flink_examples_stateful  # set job name

    transport:
        type: kafka
        topicName: input.runs
        properties:
            bootstrap.servers: localhost:9093
            security.protocol: SASL_PLAINTEXT
            sasl.mechanism: SCRAM-SHA-256
            sasl.jaas.config: |
                org.apache.kafka.common.security.scram.ScramLoginModule required
                username="data_rentgen"
                password="changeme";
            key.serializer: org.apache.kafka.common.serialization.StringSerializer
            value.serializer: org.apache.kafka.common.serialization.StringSerializer
            compression.type: zstd
            acks: all

* Pass path to config file via ``OPENLINEAGE_CONFIG`` environment variable of ``JobManager``:

  .. code:: ini

    OPENLINEAGE_CONFIG=/path/to/openlineage.yml

Collect and send lineage
------------------------

Just start your Flink job. OpenLineage integration will automatically collect and send lineage to DataRentgen.

See results
-----------

Browse frontend pages `Jobs <http://localhost:3000/jobs>`_ to see what information was extracted by OpenLineage & DataRentgen.

Job list page
~~~~~~~~~~~~~

.. image:: ./job_list.png

Job details page
~~~~~~~~~~~~~~~~

.. image:: ./job_details.png

Run details page
~~~~~~~~~~~~~~~~

.. image:: ./run_details.png

Dataset level lineage
~~~~~~~~~~~~~~~~~~~~~

.. image:: ./dataset_lineage.png

Job level lineage
~~~~~~~~~~~~~~~~~

.. image:: ./job_lineage.png

Run level lineage
~~~~~~~~~~~~~~~~~

.. image:: ./run_lineage.png
