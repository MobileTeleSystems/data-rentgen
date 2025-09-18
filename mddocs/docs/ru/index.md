{{ datarentgen_logo_wide }}

[![Repo Status](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip) [![Docker image](https://img.shields.io/docker/v/mtsrus/data-rentgen?sort=semver&label=docker)](https://hub.docker.com/r/mtsrus/data-rentgen) [![PyPI](https://img.shields.io/pypi/v/data-rentgen)](https://pypi.org/project/data-rentgen/) [![PyPI License](https://img.shields.io/pypi/l/data-rentgen.svg)](https://github.com/MobileTeleSystems/data-rentgen/blob/develop/LICENSE.txt) [![PyPI Python Version](https://img.shields.io/pypi/pyversions/data-rentgen.svg)](https://badge.fury.io/py/data-rentgen) [![Documentation](https://readthedocs.org/projects/data-rentgen/badge/?version=stable)](https://data-rentgen.readthedocs.io/)
[![Build Status](https://github.com/MobileTeleSystems/data-rentgen/workflows/Tests/badge.svg)](https://github.com/MobileTeleSystems/data-rentgen/actions) [![Coverage](https://codecov.io/github/MobileTeleSystems/data-rentgen/graph/badge.svg?token=s0JztGZbq3)](https://codecov.io/github/MobileTeleSystems/data-rentgen) [![pre-commit.ci](https://results.pre-commit.ci/badge/github/MobileTeleSystems/data-rentgen/develop.svg)](https://results.pre-commit.ci/latest/github/MobileTeleSystems/data-rentgen/develop)

# Что такое Data.Rentgen?

Data.Rentgen — это сервис отслеживания lineage (Data Motion Lineage), совместимый со спецификацией [OpenLineage](https://openlineage.io/).

В настоящее время мы поддерживаем получение lineage из:

* Apache Spark
* Apache Airflow
* Apache Hive
* Apache Flink
* dbt

**Примечание**: сервис находится в активной разработке, поэтому пока не имеет стабильного API.

# Цели

* Собирать события lineage, создаваемые клиентами и интеграциями OpenLineage.
* Хранить события с детальностью до операций для большей точности (вместо детализации до задач, как в [Marquez](https://marquezproject.ai/)).
* Предоставлять API для получения lineage как для случаев задача/запуск ↔ датасет, так и датасет ↔ датасет.

# Возможности

* Поддержка обработки больших объемов событий lineage с использованием Apache Kafka в качестве буфера событий.
* Хранение данных в таблицах, разделенных по временным меткам событий, для ускорения построения графа lineage.
* Граф lineage строится с пользовательскими временными границами (в отличие от Marquez, где граф строится только для последнего запуска задачи).
* Граф lineage может строиться с различной степенью детализации. Например, объединение всех отдельных команд Spark в applicationId или applicationName Spark.
* Поддержка lineage на уровне колонок.
* Поддержка аутентификации.

# Не цели

* Это **НЕ** каталог данных. DataRentgen не отслеживает изменения схем датасетов, владельцев и т.д. Вместо этого используйте [Datahub](https://datahubproject.io/) или [OpenMetadata](https://open-metadata.org/).
* Статическое происхождение данных, такое как представление → таблица, не поддерживается.

# Ограничения

* OpenLineage имеет интеграции с Trino, Debezium и другими источниками.
* В отличие от Marquez, DataRentgen анализирует только ограниченный набор фасетов, отправляемых OpenLineage, и не сохраняет пользовательские фасеты. Это может быть изменено в будущем.

# Скриншоты

## Граф lineage

Граф lineage на уровне наборов данных (dataset)

![Граф lineage на уровне dataset](entities/dataset_lineage.png)

Граф lineage на уровне колонок наборов данных (dataset)

![Граф lineage на уровне колонок dataset](entities/dataset_column_lineage.png)

Граф lineage на уровне заданий (Job)

![Граф lineage на уровне Job](entities/job_lineage.png)

Граф lineage на уровне запусков Run

![Граф lineage на уровне Run](entities/run_lineage.png)

## Наборы данных (dataset)

![Список наборов данных](entities/dataset_list.png)

## Запуски (Run)

![Список запусков (Run)](entities/run_list.png)

## Приложение Spark

![Детали приложения Spark](integrations/spark/job_details.png)

## Запуск Spark

![Детали запуска Spark](integrations/spark/run_details.png)

## Команда Spark

![Детали команды Spark](integrations/spark/operation_details.png)

## Запрос Hive

![Детали запроса Hive](integrations/hive/operation_details.png)

## DagRun Airflow

![Детали DagRun Airflow](integrations/airflow/dag_run_details.png)

## TaskInstance Airflow

![Детали TaskInstance Airflow](integrations/airflow/task_run_details.png)
