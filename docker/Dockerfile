FROM python:3.13-slim AS builder

WORKDIR /app
ENV POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        autoconf \
        gcc \
        make \
        # required to build gssapi from sources
        libkrb5-dev \
    && rm -rf /var/lib/apt/lists/* /var/cache/*

RUN pip install poetry

COPY ./pyproject.toml ./poetry.lock* ./

RUN --mount=type=cache,target=/root/.cache/pypoetry \
    poetry install \
        --no-root \
        --all-extras \
        --without dev,test,docs


FROM python:3.13-slim AS prod
LABEL maintainer="DataOps.ETL"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        # required for GSSAPI Kafka auth
        krb5-user \
        # Used for healthcheck
        curl \
    && rm -rf /var/lib/apt/lists/* /var/cache/*

WORKDIR /app
ENV PYTHONPATH=/app \
    PATH="/app/.venv/bin:$PATH" \
    VIRTUAL_ENV=/app/.venv \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Logos
COPY ./docs/_static/*.svg ./data_rentgen/server/static/
# Swagger UI
ADD https://cdn.jsdelivr.net/npm/swagger-ui-dist@latest/swagger-ui-bundle.js https://cdn.jsdelivr.net/npm/swagger-ui-dist@latest/swagger-ui.css /app/data_rentgen/server/static/swagger/
# Redoc
ADD https://cdn.jsdelivr.net/npm/redoc@latest/bundles/redoc.standalone.js /app/data_rentgen/server/static/redoc/redoc.standalone.js

ENV DATA_RENTGEN__SERVER__OPENAPI__SWAGGER__JS_URL=/static/swagger/swagger-ui-bundle.js \
    DATA_RENTGEN__SERVER__OPENAPI__SWAGGER__CSS_URL=/static/swagger/swagger-ui.css \
    DATA_RENTGEN__SERVER__OPENAPI__REDOC__JS_URL=/static/redoc/redoc.standalone.js \
    DATA_RENTGEN__SERVER__STATIC_FILES__DIRECTORY=/app/data_rentgen/server/static

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
COPY ./data_rentgen/ ./data_rentgen/
RUN python -m compileall -b data_rentgen
