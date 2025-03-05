.. _overview-setup-spark:

Apache Spark integration
========================

Requirements
------------

* `Apache Spark <https://spark.apache.org/>`_ 3.x or higher
* `OpenLineage integration for Spark <https://openlineage.io/docs/integrations/spark/>`_ 1.23.0 or higher

Setup
-----

* Add OpenLineage integration package, and setup Spark listener in Spark session config:

  .. code:: python

      from pyspark.sql import SparkSession

      spark = (
          SparkSession.builder
          # install OpenLineage integration and Kafka client
          .config(
              "spark.jars.packages",
              "io.openlineage:openlineage-spark_2.12:1.27.0,org.apache.kafka:kafka-clients:3.9.0",
          )
          .config(
              "spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener"
          )
          # set Spark session master & applicationName
          .master("local")
          .appName("mysession")
          # set here location of Spark session, e.g. current host, YARN cluster or K8s cluster:
          .config("spark.openlineage.namespace", "local://hostname.as.fqdn")
          # .config("spark.openlineage.namespace", "yarn://some-cluster")
          # .config("spark.openlineage.namespace", "k8s://some-cluster")
          .config("spark.openlineage.transport.type", "kafka")
          # set here Kafka connection address & credentials
          .config("spark.openlineage.transport.topicName", "input.runs")
          .config(
              "spark.openlineage.transport.properties.bootstrap.servers", "localhost:9093"
          )
          .config(
              "spark.openlineage.transport.properties.security.protocol", "SASL_PLAINTEXT"
          )
          .config("spark.openlineage.transport.properties.sasl.mechanism", "SCRAM-SHA-256")
          .config(
              "spark.openlineage.transport.properties.sasl.jaas.config",
              'org.apache.kafka.common.security.scram.ScramLoginModule required username="data_rentgen" password="changeme";',
          )
          .config("spark.openlineage.transport.properties.acks", "all")
          .config(
              "spark.openlineage.transport.properties.key.serializer",
              "org.apache.kafka.common.serialization.StringSerializer",
          )
          .config(
              "spark.openlineage.transport.properties.value.serializer",
              "org.apache.kafka.common.serialization.StringSerializer",
          )
          .config("spark.openlineage.transport.properties.compression.type", "zstd")
          # few other important options
          .config("spark.openlineage.jobName.appendDatasetName", "false")
          .config("spark.openlineage.columnLineage.datasetLineageEnabled", "true")
          .getOrCreate()
      )

Collect and send lineage
------------------------

* Perform some data operations using Spark, like:

.. code:: python

    df = spark.read.format("jdbc").options(...).load()
    df.write.format("csv").save("/output/path")


* After ETL pipeline is finished, **stop** Spark session (this is required to change Spark run status to ``SUCCEEDED``):

.. code:: python

    spark.stop()

See results
-----------

* Browse frontend pages `Datasets <http://localhost:3000/#/datasets>`_ and `Jobs <http://localhost:3000/#/jobs>`_
  to see what information was extracted by OpenLineage & DataRentgen.
