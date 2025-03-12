FROM python:3.13-slim AS prod
LABEL maintainer="DataOps.ETL"

WORKDIR /app
ENV PYTHONPATH=/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        autoconf \
        gcc \
        make \
        # required to build gssapi from sources
        libkrb5-dev \
        krb5-user \
        # curl is used for healthcheck
        curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install poetry \
    && poetry config virtualenvs.create false

COPY ./pyproject.toml ./poetry.lock* ./

RUN --mount=type=cache,target=/root/.cache/pypoetry \
    poetry install \
        --no-root \
        --all-extras \
        --without dev,test,docs

# Swagger UI
ADD https://cdn.jsdelivr.net/npm/swagger-ui-dist@latest/swagger-ui-bundle.js /app/data_rentgen/server/static/swagger/swagger-ui-bundle.js
ADD https://cdn.jsdelivr.net/npm/swagger-ui-dist@latest/swagger-ui.css /app/data_rentgen/server/static/swagger/swagger-ui.css

# Redoc
ADD https://cdn.jsdelivr.net/npm/redoc@latest/bundles/redoc.standalone.js /app/data_rentgen/server/static/redoc/redoc.standalone.js

ENV DATA_RENTGEN__SERVER__OPENAPI__SWAGGER__JS_URL=/static/swagger/swagger-ui-bundle.js \
    DATA_RENTGEN__SERVER__OPENAPI__SWAGGER__CSS_URL=/static/swagger/swagger-ui.css \
    DATA_RENTGEN__SERVER__OPENAPI__REDOC__JS_URL=/static/redoc/redoc.standalone.js \
    DATA_RENTGEN__SERVER__STATIC_FILES__DIRECTORY=/app/data_rentgen/server/static

COPY ./docs/_static/*.svg ./data_rentgen/server/static/
COPY ./data_rentgen/ ./data_rentgen/
