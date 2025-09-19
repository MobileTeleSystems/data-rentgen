# Провайдер Keycloak { #auth-server-keycloak }

## Описание

Провайдер аутентификации Keycloak использует библиотеку [python-keycloak](https://pypi.org/project/python-keycloak/) для взаимодействия с сервером Keycloak. В процессе аутентификации KeycloakAuthProvider перенаправляет пользователя на страницу аутентификации Keycloak.

После успешной аутентификации Keycloak перенаправляет пользователя обратно в Data.Rentgen с кодом авторизации.
Затем KeycloakAuthProvider обменивает код авторизации на токен доступа и использует его для получения информации о пользователе с сервера Keycloak.
Если пользователь не найден в базе данных Data.Rentgen, KeycloakAuthProvider создает его. Наконец, KeycloakAuthProvider возвращает пользователя с токеном доступа.

## Схема взаимодействия

```plantuml title="Схема взаимодействия"

        @startuml
            title DummyAuthProvider
            participant "Frontend"
            participant "Backend"
            participant "Keycloak"

            == Аутентификация Frontend в Keycloak ==

            Frontend -> Backend : Запрос к эндпоинту с аутентификацией (/v1/locations)

            Backend x-[#red]> Frontend: 401 с URL перенаправления в поле 'details' ответа

            Frontend -> Keycloak : Перенаправление пользователя на страницу входа Keycloak

            alt Успешный вход
                Frontend --> Keycloak : Вход с логином и паролем
            else Ошибка входа
                Keycloak x-[#red]> Frontend -- : Отображение ошибки (401 Unauthorized)
            end

            Keycloak -> Frontend : Обратный вызов к Frontend /callback который является прокси между Keycloak и Backend

            Frontend -> Backend : Отправка запроса к Backend '/v1/auth/callback'

            Backend -> Keycloak : Проверка исходного 'state' и обмен кода на токены
            Keycloak --> Backend : Возврат токенов
            Backend --> Frontend : Установка токенов в браузере пользователя в cookies

            Frontend --> Backend : Запрос к /v1/locations с сессионными cookies
            Backend -> Backend : Получение информации о пользователе из токена и проверка пользователя во внутренней базе данных backend
            Backend -> Backend : Создание пользователя во внутренней базе данных backend, если не существует
            Backend -[#green]> Frontend -- : Возврат запрашиваемых данных


            == GET v1/datasets ==


            alt Успешный случай
                "Frontend" -> "Backend" ++ : access_token
                "Backend" --> "Backend" : Проверка токена
                "Backend" --> "Backend" : Проверка пользователя во внутренней базе данных backend
                "Backend" -> "Backend" : Получение данных
                "Backend" -[#green]> "Frontend" -- : Возврат данных

            else Токен истек (Успешный случай)
                "Frontend" -> "Backend" ++ : access_token, refresh_token
                "Backend" --> "Backend" : Проверка токена
                "Backend" -[#yellow]> "Backend" : Токен истек
                "Backend" --> "Keycloak" : Попытка обновления токена
                "Backend" --> "Backend" : Проверка нового токена
                "Backend" --> "Backend" : Проверка пользователя во внутренней базе данных backend
                "Backend" -> "Backend" : Получение данных
                "Backend" -[#green]> "Frontend" -- : Возврат данных

            else Создание нового пользователя
                "Frontend" -> "Backend" ++ : access_token
                "Backend" --> "Backend" : Проверка токена
                "Backend" --> "Backend" : Проверка пользователя во внутренней базе данных backend
                "Backend" --> "Backend" : Создание нового пользователя
                "Backend" -> "Backend" : Получение данных
                "Backend" -[#green]> "Frontend" -- : Возврат данных

            else Токен истек и плохой refresh token
                "Frontend" -> "Backend" ++ : access_token, refresh_token
                "Backend" --> "Backend" : Проверка токена
                "Backend" -[#yellow]> "Backend" : Токен истек
                "Backend" --> "Keycloak" : Попытка обновления токена
                "Backend" x-[#red]> "Frontend" -- : RedirectResponse не может обновить

            else Неверная полезная нагрузка токена
                "Frontend" -> "Backend" ++ : access_token, refresh_token
                "Backend" --> "Backend" : Проверка токена
                "Backend" x-[#red]> "Frontend" -- : 307 Ошибка авторизации

            end

            deactivate "Frontend"
        @enduml

```

## Базовая конфигурация

::: data_rentgen.server.settings.auth.keycloak.KeycloakAuthProviderSettings

::: data_rentgen.server.settings.auth.keycloak.KeycloakSettings
