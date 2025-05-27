.. toctree::
    :maxdepth: 2
    :caption: Data.Rentgen
    :hidden:

    self

.. toctree::
    :maxdepth: 2
    :caption: Overview
    :hidden:

    quickstart/index
    entities

.. toctree::
    :maxdepth: 2
    :caption: Reference
    :hidden:

    reference/architecture
    reference/database/index
    reference/broker/index
    reference/consumer/index
    reference/server/index
    reference/frontend/index

.. toctree::
    :maxdepth: 2
    :caption: Development
    :hidden:

    changelog
    contributing
    security

.. include:: ../README.rst
    :end-before: |Logo|

.. include raw <svg> instead of <image source=".svg"> to make attribute fill="..." change text color depending on documentation theme
.. raw:: html
    :file: _static/logo_wide.svg

.. include:: ../README.rst
    :start-after: |Logo|
    :end-before: documentation

Screenshots
-----------

Lineage graph
~~~~~~~~~~~~~

Dataset downstream lineage

.. image:: quickstart/spark/dataset_downstream_lineage.png
    :alt: Dataset downstream lineage

Dataset upstream lineage

.. image:: quickstart/spark/dataset_upstream_lineage.png
    :alt: Dataset upstream lineage

Direct column-level lineage

.. image:: quickstart/spark/dataset_direct_column_lineage.png
    :alt: Dataset direct column-level lineage graph

Inirect column-level lineage

.. image:: quickstart/spark/dataset_indirect_column_lineage.png
    :alt: Dataset indirect column-level lineage graph

Datasets
~~~~~~~~

.. image:: quickstart/dataset_list.png
    :alt: Datasets list

Runs
~~~~

.. image:: quickstart/run_list.png
    :alt: Runs list

Spark application
~~~~~~~~~~~~~~~~~

.. image:: quickstart/spark/job_details.png
    :alt: Spark application details

Spark run
~~~~~~~~~

.. image:: quickstart/spark/run_details.png
    :alt: Spark run details

Spark operation
~~~~~~~~~~~~~~~

.. image:: quickstart/spark/operation_details.png
    :alt: Spark operation details

Airflow DagRun
~~~~~~~~~~~~~~~

.. image:: quickstart/airflow/dag_run_details.png
    :alt: Airflow DagRun details

Airflow TaskInstance
~~~~~~~~~~~~~~~~~~~~~

.. image:: quickstart/airflow/task_run_details.png
    :alt: Airflow TaskInstance details
