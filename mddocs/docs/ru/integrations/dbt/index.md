# Интеграция с dbt { #overview-setup-dbt }

Использование [интеграции OpenLineage с dbt](https://openlineage.io/docs/integrations/dbt).

## Требования

- [dbt](https://www.getdbt.com/) 1.3 или выше
- OpenLineage 1.19.0 или выше, рекомендуется 1.34.0+

## Отображение сущностей

- dbt проект → Data.Rentgen Job
- dbt запуск → Data.Rentgen Run
- dbt модель, снапшот, sql, тест → Data.Rentgen Operation

## Установка

```console
$ pip install "openlineage-dbt>=1.34.0" "openlineage-python[kafka]>=1.34.0" zstd
...
```

## Настройка

- Создайте файл `openlineage.yml` с содержимым ниже:

  ```yaml
  transport:
      type: kafka
      topic: input.runs
      config:
          bootstrap.servers: localhost:9093
          security.protocol: SASL_PLAINTEXT
          sasl.mechanism: SCRAM-SHA-256
          sasl.username: data_rentgen
          sasl.password: changeme
          compression.type: zstd
          acks: all
  ```

- Установите переменные окружения:

  ```ini
  OPENLINEAGE_NAMESPACE=local://dbt.host.name
  OPENLINEAGE_CONFIG=/path/to/openlineage.yml
  ```

## Сбор и отправка lineage

Замените команды CLI `dbt`:

```shell
$ dbt run myproject
...
$ dbt test myproject
...
```

на CLI `dbt-ol`:

```shell
$ dbt-ol run myproject
...
$ dbt-ol test myproject
...
```

Lineage будет автоматически отправлен в Data.Rentgen интеграцией OpenLineage.

## Просмотр результатов

Просмотрите страницу интерфейса [Jobs](http://localhost:3000/jobs), чтобы увидеть, какая информация была извлечена OpenLineage и DataRentgen

### Страница списка Job

![список заданий](job_list.png)

### Страница сведений о Job

![сведения о задании](job_details.png)

### Lineage уровня Job

![lineage задания](job_lineage.png)

### Сведения о запуске (Run)

![сведения о запуске](run_details.png)

### Lineage запуска (Run)

![lineage запуска](run_lineage.png)

### Сведения об операции

![сведения об операции](operation_details.png)

### Lineage операции

![lineage операции](operation_lineage.png)
