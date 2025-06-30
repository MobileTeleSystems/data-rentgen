.. _clean-partitions-cli:

CLI for cleaning partitions data
================================
This script is designed to manage PostgreSQL table partitions by providing functionalities to list, detach, remove, or truncate old partitions.

::

 usage: python3 -m data_rentgen.db.scripts.clean_partitions --command truncate --keep-after $(date -v2m '+%Y-%m-%d')


The ``clean_partitions.py`` script helps automate the cleanup of old PostgreSQL table partitions based on a specified keep-after date. It supports different commands for dry runs, detaching partitions, removing data, and truncating partitions.
It's automatically inditifies partitioned tables and their granularity.

Arguments
~~~~~~~~~

* ``command``: (Optional) Specifies the operation mode.
    * Choices: ``dry_run``, ``detach_partitions``, ``remove_data``, ``truncate``
    * Default: ``dry_run``
    * Description:
        * ``dry_run``: Logs the names of partitions that would be affected by the cleanup without executing any SQL commands.

        * ``detach_partitions``: Generates and executes ``ALTER TABLE ... DETACH PARTITION ...`` commands for identified old partitions. This makes the partitions independent tables but keeps their data.

        * ``remove_data``: First detaches partitions, then generates and executes ``DROP TABLE ...`` commands, permanently deleting the partition tables and their data.

        * ``truncate``: Generates and executes ``TRUNCATE TABLE ...`` commands, removing all rows from the identified old partition tables but keeping the table structure. **This option is preferred if you have streaming data**.

* ``--keep-after``: (Optional) The cut-off date for partitions. Partitions with data before this date will be considered for cleanup.
    * Type: Date (e.g., ``YYYY-MM-DD``). The script uses isoparse for parsing, so various ISO formats are supported.

    * Default: The current date - 1 day.

    * Description: Only partitions whose date components are strictly before this specified date will be processed taking into account granularity of the table.

Examples
~~~~~~~~

1. Perform a Dry Run (default):

::

    python3 -m data_rentgen.db.scripts.clean_partitions command dry_run --keep-after 2024-01-01

This command will log which partitions would be affected if you were to clean up partitions older than January 1, 2024, without making any changes to your database.

2. Detach Partitions Older Than a Specific Date:

::

    python3 -m data_rentgen.db.scripts.clean_partitions command detach_partitions --keep-after 2024-01-01

This will detach all partitions created before January 1, 2024, from their parent tables. The detached tables will still exist with their data.

3. Remove Data and Drop Partitions Older Than a Specific Date:

::

    python3 -m data_rentgen.db.scripts.clean_partitions command remove_data --keep-after 2024-06-01

This will detach and then **drop all partitions** created before January 1, 2024, permanently deleting their data.

4. Truncate Data in Partitions Older Than a Specific Date:

This option is preferred with streaming ``Jobs``

::

    python3 -m data_rentgen.db.scripts.clean_partitions command truncate --keep-after 2023-01-01

This will delete all rows from partitions created before January 1, 2023, but will keep the empty partition tables.
