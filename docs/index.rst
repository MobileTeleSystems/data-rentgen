.. include:: ../README.rst
    :end-before: |Logo|

.. include raw <svg> instead of <image source=".svg"> to make attribute fill="..." change text color depending on documentation theme
.. raw:: html
    :file: _static/logo_wide.svg

.. include:: ../README.rst
    :start-after: |Logo|
    :end-before: documentation

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
