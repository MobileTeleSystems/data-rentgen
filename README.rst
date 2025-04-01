.. _readme:

|Logo|

.. |Logo| image:: docs/_static/logo_wide_white_text.svg
    :alt: Data.Rentgen logo
    :target: https://github.com/MobileTeleSystems/data-rentgen

|Repo Status| |PyPI| |PyPI License| |PyPI Python Version| |Docker image| |Documentation|
|Build Status| |Coverage| |pre-commit.ci|

.. |Repo Status| image:: https://www.repostatus.org/badges/latest/wip.svg
    :target: https://www.repostatus.org/#wip
.. |PyPI| image:: https://img.shields.io/pypi/v/data-rentgen
    :target: https://pypi.org/project/data-rentgen/
.. |PyPI License| image:: https://img.shields.io/pypi/l/data-rentgen.svg
    :target: https://github.com/MobileTeleSystems/data-rentgen/blob/develop/LICENSE.txt
.. |PyPI Python Version| image:: https://img.shields.io/pypi/pyversions/data-rentgen.svg
    :target: https://badge.fury.io/py/data-rentgen
.. |Docker image| image:: https://img.shields.io/docker/v/mtsrus/data-rentgen?sort=semver&label=docker
    :target: https://hub.docker.com/r/mtsrus/data-rentgen
.. |Documentation| image:: https://readthedocs.org/projects/data-rentgen/badge/?version=stable
    :target: https://data-rentgen.readthedocs.io/
.. |Build Status| image:: https://github.com/MobileTeleSystems/data-rentgen/workflows/Tests/badge.svg
    :target: https://github.com/MobileTeleSystems/data-rentgen/actions
.. |Coverage| image:: https://codecov.io/github/MobileTeleSystems/data-rentgen/graph/badge.svg?token=s0JztGZbq3
    :target: https://codecov.io/github/MobileTeleSystems/data-rentgen
.. |pre-commit.ci| image:: https://results.pre-commit.ci/badge/github/MobileTeleSystems/data-rentgen/develop.svg
    :target: https://results.pre-commit.ci/latest/github/MobileTeleSystems/data-rentgen/develop

What is Data.Rentgen?
---------------------

Data.Rentgen is a Data Motion Lineage service, compatible with `OpenLineage <https://openlineage.io/>`_ specification.

**Note**: service is under active development, and is not ready to use yet.

Goals
-----

* Collect lineage events produced by OpenLineage clients & integrations.
* Store operation-grained events for better detalization (instead of job grained `Marquez <https://marquezproject.ai/>`_).
* Provide API for fetching job/run ↔ dataset lineage, not dataset ↔ dataset lineage (like `Datahub <https://datahubproject.io/>`_ and `OpenMetadata <https://open-metadata.org/>`_).

Features
--------

* Support consuming large amounts of lineage events, use Apache Kafka as event buffer.
* Store data in tables partitioned by event timestamp, to speed up lineage graph resolution.
* Lineage graph is build with user-specified time boundaries (unlike Marquez where lineage is build only for last job run).
* Lineage graph can be build with different granularity. e.g. merge all individual Spark operations into Spark applicationId or Spark applicationName.
* Column-level lineage support.
* Authentication support.

Non-goals
---------

* This is **not** a Data Catalog, DataRentgen doesn't track dataset schema change, owner and so on. Use Datahub or OpenMetadata instead.
* Static Data Lineage like view → table is not supported.

Limitations
-----------

* For now, only Apache Spark and Apache Airflow are supported as lineage event sources.
  OpenLineage also supports Apache Flink, DBT, Trino and others. DataRentgen support may be added later.
* Unlike Marquez, DataRentgen parses only limited set of facets send by OpenLineage, and doesn't store custom facets. This can be changed in future.

.. documentation

Documentation
-------------

See https://data-rentgen.readthedocs.io/
