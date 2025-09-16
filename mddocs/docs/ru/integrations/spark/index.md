# Интеграция с Apache Spark { #overview-setup-spark }

Использование [OpenLineage интеграции с Apache Spark](https://openlineage.io/docs/integrations/spark/).

## Требования

- [Apache Spark](https://spark.apache.org/) 3.x или выше
- OpenLineage 1.23.0 или выше, рекомендуется 1.34.0+

## Соответствие сущностей

- Spark applicationName → Data.Rentgen Job
- Spark applicationId → Data.Rentgen Run
- Spark job, execution, RDD → Data.Rentgen Operation

## Настройка

### Через файл конфигурации OpenLineage

- Создайте файл `openlineage.yml` с содержимым:

  ```yaml openlineage.yml
  transport:
      type: kafka
      topicName: input.runs
      properties:
          bootstrap.servers: localhost:9093
          security.protocol: SASL_PLAINTEXT
          sasl.mechanism: SCRAM-SHA-256
          sasl.jaas.config: |
              org.apache.kafka.common.security.scram.ScramLoginModule required
              username="data_rentgen"
              password="changeme";
          key.serializer: org.apache.kafka.common.serialization.StringSerializer
          value.serializer: org.apache.kafka.common.serialization.StringSerializer
          compression.type: zstd
          acks: all
  ```

- Передайте путь к файлу конфигурации через переменную окружения `OPENLINEAGE_CONFIG`:

  ```ini
  OPENLINEAGE_NAMESPACE=local://hostname.as.fqdn
  OPENLINEAGE_CONFIG=/path/to/openlineage.yml
  ```

- Настройте `OpenLineageSparkListener` через конфигурацию SparkSession:

```python spark_config.py
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    # установка интеграции OpenLineage и клиента Kafka
    .config(
        "spark.jars.packages",
        "io.openlineage:openlineage-spark_2.12:1.34.0,org.apache.kafka:kafka-clients:3.9.0",
    )
    .config(
        "spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener"
    )
    # установка master и applicationName для Spark сессии
    .master("local")
    .appName("mysession")
    # несколько других важных опций
    .config("spark.openlineage.jobName.appendDatasetName", "false")
    .config("spark.openlineage.columnLineage.datasetLineageEnabled", "true")
    .getOrCreate()
)
```

### Через конфигурацию `SparkSession`

Добавьте пакет интеграции OpenLineage, настройте `OpenLineageSparkListener` в конфигурации SparkSession:

```python spark_session_config.py
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    # установка интеграции OpenLineage и клиента Kafka
    .config(
        "spark.jars.packages",
        "io.openlineage:openlineage-spark_2.12:1.34.0,org.apache.kafka:kafka-clients:3.9.0",
    )
    .config(
        "spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener"
    )
    # установка master и applicationName для Spark сессии
    .master("local")
    .appName("mysession")
    # укажите здесь местоположение Spark сессии, например, текущий хост, YARN кластер или K8s кластер:
    .config("spark.openlineage.namespace", "local://hostname.as.fqdn")
    # .config("spark.openlineage.namespace", "yarn://some-cluster")
    # .config("spark.openlineage.namespace", "k8s://some-cluster")
    .config("spark.openlineage.transport.type", "kafka")
    # укажите здесь адрес подключения к Kafka и учетные данные
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
    # несколько других важных опций
    .config("spark.openlineage.jobName.appendDatasetName", "false")
    .config("spark.openlineage.columnLineage.datasetLineageEnabled", "true")
    .getOrCreate()
)
```

## Сбор и отправка данных о происхождении

- Используйте `SparkSession` как менеджер контекста, чтобы правильно перехватывать события остановки сессии:

```python spark_context.py
with SparkSession.builder.getOrCreate() as spark:
    # работа со spark внутри этого контекста
```

- Выполните некоторые операции с данными с помощью Spark, например:

```python spark_operations.py
df = spark.read.format("jdbc").options(...).load()
df.write.format("csv").save("/output/path")
```

Данные о lineage будут автоматически отправлены в Data.Rentgen с помощью `OpenLineageSparkListener`.

## Просмотр результатов

Просмотрите страницу [Jobs](http://localhost:3000/jobs) в интерфейсе, чтобы увидеть, какая информация была извлечена OpenLineage и DataRentgen.

### Страница списка заданий (Job)

![список заданий (Job)](job_list.png)

### Страница детализации задания (Job)

![детали задания (Job)](job_details.png)

### Страница детальной информации о запуске (Run)

![детали запуска (Run)](run_details.png)

### Страница детальной информации об операции

![детали операции](operation_details.png)

### Lineage на уровне набора данных (dataset)

![dataset lineage](dataset_lineage.png)

![column lineage](dataset_column_lineage.png)

### Lineage на уровне задания (Job)

![job lineage](job_lineage.png)

### Lineage на уровне запуска (Run)

![run lineage](run_lineage.png)

### Lineage на уровне операции

![operation lineage](operation_lineage.png)
