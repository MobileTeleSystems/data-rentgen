# Настройка отладки { #configuration-server-debug }

## Возврат отладочной информации в ответах REST API

По умолчанию сервер не добавляет детали ошибок в тело ответа, чтобы избежать раскрытия специфичной информации экземпляра конечным пользователям.

Вы можете изменить это, установив:

```console
$ export DATA_RENTGEN__SERVER__DEBUG=False
$ # запуск REST API сервера
$ curl -XPOST http://localhost:8000/failing/endpoint ...
{
    "error": {
        "code": "unknown",
        "message": "Получено необработанное исключение. Пожалуйста, обратитесь в службу поддержки",
        "details": null,
    },
}
```

```console
$ export DATA_RENTGEN__SERVER__DEBUG=True
$ # запуск REST API сервера
$ curl -XPOST http://localhost:8000/failing/endpoint ...
Traceback (most recent call last):
File ".../uvicorn/protocols/http/h11_impl.py", line 408, in run_asgi
    result = await app(  # type: ignore[func-returns-value]
            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File ".../site-packages/uvicorn/middleware/proxy_headers.py", line 84, in __call__
    return await self.app(scope, receive, send)
```

!!! warning "Предупреждение"

    Это только для среды разработки. **НЕ** используйте в продакшене!

## Вывод отладочных логов в бэкенде

См. [`Настройка логирования`][configuration-server-logging], но замените уровень логирования `INFO` на `DEBUG`.

## Заполнение заголовка `X-Request-ID` в бэкенде

Сервер может добавлять заголовок `X-Request-ID` к ответам, что позволяет сопоставить запрос клиента с ответом бэкенда.

Это делается middleware `request_id`, который включен по умолчанию и может быть настроен как описано ниже:

::: data_rentgen.server.settings.request_id.RequestIDSettings

## Вывод ID запроса в логи бэкенда

Это делается добавлением специфичного фильтра к обработчику логирования:

??? note "logging.yml"

    ```yaml hl_lines="6-12 23-24 35"
    ----8<----
    data_rentgen/logging/presets/plain.yml
    ----8<----
    ```

Результирующие логи выглядят так:

```text
2023-12-18 17:14:11.711 uvicorn.access:498 [INFO] 018c15e97a068ae09484f8c25e2799dd 127.0.0.1:34884 - "GET /monitoring/ping HTTP/1.1" 200
```

## Использование заголовка `X-Request-ID` в клиенте

Если клиент получил заголовок `X-Request-ID` от бэкенда, он выводится в логи с уровнем `DEBUG`:

```pycon
>>> import logging
>>> logging.basicConfig(level=logging.DEBUG)
>>> client.ping()
DEBUG:urllib3.connectionpool:http://localhost:8000 "GET /monitoring/ping HTTP/1.1" 200 15
DEBUG:data_rentgen.client.base:Request ID: '018c15e97a068ae09484f8c25e2799dd'
```

Также, если ответ REST API был неуспешным, `Request ID` добавляется к сообщению исключения:

```pycon
>>> client.get_namespace("unknown")
requests.exceptions.HTTPError: 404 Client Error: Not Found for url: http://localhost:8000/v1/namespaces/unknown
Request ID: '018c15eb80fa81a6b38c9eaa519cd322'
```

## Заполнение заголовка `X-Application-Version` на стороне REST API

Сервер может добавлять заголовок `X-Application-Version` к ответам, что позволяет определить, какая версия бэкенда развернута.

Это делается middleware `application_version`, который включен по умолчанию и может быть настроен как описано ниже:

::: data_rentgen.server.settings.application_version.ApplicationVersionSettings
