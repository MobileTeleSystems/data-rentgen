# Конфигурация фронтенда { #configuration-frontend }

## URL API

Для работы интерфейса Data.Rentgen требуется доступ к REST API из браузера. URL API настраивается с помощью переменной окружения:

```bash
DATA_RENTGEN__UI__API_BROWSER_URL=http://localhost:8000
```

Если REST API и фронтенд обслуживаются на одном домене (например, через обратный прокси Nginx), например:

- REST API → `/api`
- Фронтенд → `/`

Тогда вы можете использовать относительный путь:

```bash
DATA_RENTGEN__UI__API_BROWSER_URL=/api
```
