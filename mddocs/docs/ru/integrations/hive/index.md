# Интеграция с Apache Hive { #overview-setup-hive }

Использование [интеграции OpenLineage с Apache Hive](https://openlineage.io/docs/integrations/hive/).

## Требования

- [Apache Hive](https://hive.apache.org/) 3.1.3 (версия 4.0 пока не поддерживается)
- OpenLineage 1.34.0 или выше, рекомендуется 1.35.0+

## Ограничения

- **Hive CLI** не поддерживается. Требуется HiveServer2.

- В версии OpenLineage 1.34.0 только следующие запросы обрабатываются как содержащие данные о lineage:

  - `CREATE TABLE .. AS SELECT ...`
  - `INSERT INTO ... SELECT ...`

  Другие типы запросов игнорируются интеграцией OpenLineage, включая:

  - `CREATE TABLE ...`, `ALTER TABLE ...`, `TRUNCATE TABLE ...`, `DROP TABLE ...`.
  - `INSERT INTO ... VALUES ...`, `UPDATE`, `DELETE`, `MERGE`.
  - `LOAD DATA`, `EXPORT`, `IMPORT`.
  - `SELECT` данных напрямую в клиент JDBC.

- Hive отправляет события при запуске пользовательской сессии, но не при ее завершении. Поэтому все сессии Hive в Data.Rentgen имеют статус `STARTED`.

## Отображение сущностей

- Пользователь Hive + IP пользователя → Задача (Job) Data.Rentgen
- Сессия Hive → Запуск (Run) Data.Rentgen
- Запрос Hive → Операция (Operation) Data.Rentgen

## Установка

Скачайте следующие JAR-файлы и поместите их в директорию `/path/to/jars/` на машине с HiveServer2:

- [openlineage-java](https://mvnrepository.com/artifact/io.openlineage/openlineage-java)
- [openlineage-hive](https://mvnrepository.com/artifact/io.openlineage/openlineage-hive)
- [kafka-clients](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients)
- [zstd-jni](https://mvnrepository.com/artifact/com.github.luben/zstd-jni)

## Настройка

Измените конфигурационный файл `hive-site.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <!-- Настройка Hive, чтобы не выдавал ошибки о неизвестных свойствах -->
    <property>
        <name>hive.conf.validation</name>
        <value>false</value>
    </property>

    <!-- Укажите путь к скачанным JAR-файлам -->
    <property>
        <name>hive.aux.jars.path</name>
        <value>/path/to/jars/</value>
    </property>

    <!-- Включение интеграции OpenLineage на основе хуков Hive -->
    <property>
        <name>hive.server2.session.hook</name>
        <value>io.openlineage.hive.hooks.HiveOpenLineageHook</value>
    </property>
    <property>
        <name>hive.exec.post.hooks</name>
        <value>io.openlineage.hive.hooks.HiveOpenLineageHook</value>
    </property>
    <property>
        <name>hive.exec.failure.hooks</name>
        <value>io.openlineage.hive.hooks.HiveOpenLineageHook</value>
    </property>

    <!-- Настройка транспорта OpenLineage через Kafka -->
    <property>
        <name>hive.openlineage.transport.type</name>
        <value>kafka</value>
    </property>
    <property>
        <name>hive.openlineage.transport.topicName</name>
        <value>input.runs</value>
    </property>
    <property>
        <name>hive.openlineage.transport.properties.bootstrap.servers</name>
        <!-- Адрес должен быть доступен с HiveServer2 -->
        <value>localhost:9093</value>
    </property>
    <property>
        <name>hive.openlineage.transport.properties.security.protocol</name>
        <value>SASL_PLAINTEXT</value>
    </property>
    <property>
        <name>hive.openlineage.transport.properties.sasl.mechanism</name>
        <value>SCRAM-SHA-256</value>
    </property>
    <property>
        <name>hive.openlineage.transport.properties.sasl.jaas.config</name>
        <value>org.apache.kafka.common.security.scram.ScramLoginModule required username="data_rentgen" password="changeme";</value>
    </property>
    <property>
        <name>hive.openlineage.transport.properties.key.serializer</name>
        <value>org.apache.kafka.common.serialization.StringSerializer</value>
    </property>
    <property>
        <name>hive.openlineage.transport.properties.value.serializer</name>
        <value>org.apache.kafka.common.serialization.StringSerializer</value>
    </property>
    <property>
        <name>hive.openlineage.transport.properties.compression.type</name>
        <value>zstd</value>
    </property>
    <property>
        <name>hive.openlineage.transport.properties.acks</name>
        <value>all</value>
    </property>

    <!-- Установка пространства имен по умолчанию для задач -->
    <property>
        <name>hive.openlineage.namespace</name>
        <value>hive://my.hive.host:10000</value>
    </property>
</configuration>
```

## Сбор и отправка данных о lineage

Подключитесь к интерфейсу JDBC вашего экземпляра HiveServer2, например, с помощью `beeline` или DBeaver.
После выполнения запроса интеграция отправит события о lineage в DataRentgen.

!!! note

    По умолчанию задача (Job) создается с именем `{username}@{clientIp}`. Вы можете переопределить это имя, выполнив следующую команду:

    ```sql
    SET hive.openlineage.job.name=my_session_name;
    ```

## Просмотр результатов

Просмотрите страницы пользовательского интерфейса [Jobs](http://localhost:3000/jobs), чтобы увидеть, какая информация была извлечена OpenLineage и DataRentgen.

### Страница списка заданий (Job)

![список заданий (Job)](job_list.png)

### Страница деталей задания (Job)

![детали задания (Job)](job_details.png)

### Страница деталей запуска (Run)

![детали запуска (run)](run_details.png)

### Страница деталей операции

![детали операции](operation_details.png)

### Lineage на уровне набора данных (dataset)

![dataset lineage](dataset_lineage.png)

### Lineage на уровне задания (Job)

![Job lineage](job_lineage.png)

### Линидж на уровне запуска (Run)

![run lineage](run_lineage.png)

### lineage на уровне операции

![lineage операции](operation_lineage.png)
