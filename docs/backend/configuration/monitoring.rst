.. _backend-configuration-monitoring:

Setup monitoring
================

Backend provides 2 endpoints with Prometheus compatible metrics:

* ``GET /monitoring/metrics`` - server metrics, like number of requests per path and response status, CPU and RAM usage, and so on.

.. dropdown:: Example

    .. literalinclude:: ../../_static/metrics.prom

These endpoints are enabled and configured using settings below:

.. autopydantic_model:: arrakis.backend.settings.server.monitoring.MonitoringSettings
