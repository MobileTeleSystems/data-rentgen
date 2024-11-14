.. _overview-setup-airflow:

Apache Airflow integration
==========================

Requirements
------------

* For Airflow 2.7 or higher, use `apache-airflow-providers-openlineage <https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/index.html>`_ 1.9.0 or higher:

  .. code:: console

    $ pip install "openlineage-python>=1.24.2" "apache-airflow-providers-openlineage>=1.13.0"

* For Airflow 2.1.x-2.7.x, use `OpenLineage integration for Airflow <https://openlineage.io/docs/integrations/airflow/>`_ 1.19.0 or higher

  .. code:: console

    $ pip install "openlineage-python>=1.24.2"

Setup
-----

* Setup OpenLineage integration using ``airflow.cfg`` config file:

  .. code:: ini

      [openlineage]
      # set here address of Airflow Web UI
      namespace = http://airflow.hostname.as.fqdn:8081
      # set here Kafka connection address & credentials
      transport = {"type": "kafka", "config": {"bootstrap.servers": "localhost:9093", "security.protocol": "SASL_PLAINTEXT", "sasl.mechanism": "PLAIN", "sasl.username": "data_rentgen", "sasl.password": "changeme", "acks": 1}, "topic": "input.runs", "flush": true}


  or using environment variables:

  .. code:: text

      AIRFLOW__OPENLINEAGE__NAMESPACE=http://airflow.hostname.as.fqdn:8081
      AIRFLOW__OPENLINEAGE__TRANSPORT={"type": "kafka", "config": {"bootstrap.servers": "localhost:9093", "security.protocol": "SASL_PLAINTEXT", "sasl.mechanism": "PLAIN", "sasl.username": "data_rentgen", "sasl.password": "changeme", "acks": 1}, "topic": "input.runs", "flush": true}

Collect and send lineage
------------------------

Run some Airflow dag with tasks, and wait until finished.

See results
-----------

* Then browse frontend page `Jobs <http://localhost:3000/#/jobs>`_ to see what information was extracted by OpenLineage & DataRentgen.
