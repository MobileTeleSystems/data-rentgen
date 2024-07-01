.. _readme:

Arrakis
=======

|Repo Status| |PyPI| |PyPI License| |PyPI Python Version| |Docker image| |Documentation|
|Build Status| |Coverage| |pre-commit.ci|

.. |Repo Status| image:: https://www.repostatus.org/badges/latest/concept.svg
    :target: https://www.repostatus.org/#concept
.. |PyPI| image:: https://img.shields.io/pypi/v/data-arrakis
    :target: https://pypi.org/project/data-arrakis/
.. |PyPI License| image:: https://img.shields.io/pypi/l/data-arrakis.svg
    :target: https://github.com/MobileTeleSystems/arrakis/blob/develop/LICENSE.txt
.. |PyPI Python Version| image:: https://img.shields.io/pypi/pyversions/data-arrakis.svg
    :target: https://badge.fury.io/py/data-arrakis
.. |Docker image| image:: https://img.shields.io/docker/v/mtsrus/arrakis-server?sort=semver&label=docker
    :target: https://hub.docker.com/r/mtsrus/arrakis-server
.. |Documentation| image:: https://readthedocs.org/projects/data-arrakis/badge/?version=stable
    :target: https://data-arrakis.readthedocs.io/
.. |Build Status| image:: https://github.com/MobileTeleSystems/arrakis/workflows/Tests/badge.svg
    :target: https://github.com/MobileTeleSystems/arrakis/actions
.. |Coverage| image:: https://codecov.io/gh/MobileTeleSystems/arrakis/graph/badge.svg?token=s0JztGZbq3
    :target: https://codecov.io/gh/MobileTeleSystems/arrakis
.. |pre-commit.ci| image:: https://results.pre-commit.ci/badge/github/MobileTeleSystems/arrakis/develop.svg
    :target: https://results.pre-commit.ci/latest/github/MobileTeleSystems/arrakis/develop

What is Arrakis?
----------------

Arrakis is a DataLineage service compatible with `OpenLineage <https://openlineage.io/>`_ specification.

**Note**: service is under active development, and is not ready to use.

Goals
-----

* Collect lineage events produced by OpenLineage clients & integrations (Spark, Airflow, Flink, custom ones).
* Store operation-grained events (instead of job grained `Marquez <https://marquezproject.ai/>`_), for better detalization.
* Provide API for run ↔ dataset lineage, as well as parent run → children run lineage.
* Support handling large amounts of lineage events, using Kafka as event buffer and storing data in tables partitioned by event timestamp.

Non-goals
---------

* This is **not** a data catalog. Use `Datahub <https://datahubproject.io/>`_ or `OpenMetadata <https://open-metadata.org/>`_ instead.
* Static dataset → dataset lineage (like view → table) is not supported.
* Currently column-level lineage is not supported.

.. documentation

Documentation
-------------

See https://data-arrakis.readthedocs.io/
