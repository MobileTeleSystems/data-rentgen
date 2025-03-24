#!make

include .env.local

VENV = .venv
PIP = ${VENV}/bin/pip
POETRY = ${VENV}/bin/poetry

# Fix docker build and docker compose build using different backends
COMPOSE_DOCKER_CLI_BUILD = 1
DOCKER_BUILDKIT = 1
# Fix docker build on M1/M2
DOCKER_DEFAULT_PLATFORM = linux/amd64

HELP_FUN = \
	%help; while(<>){push@{$$help{$$2//'options'}},[$$1,$$3] \
	if/^([\w-_]+)\s*:.*\#\#(?:@(\w+))?\s(.*)$$/}; \
    print"$$_:\n", map"  $$_->[0]".(" "x(20-length($$_->[0])))."$$_->[1]\n",\
    @{$$help{$$_}},"\n" for keys %help; \

all: help

help: ##@Help Show this help
	@echo -e "Usage: make [target] ...\n"
	@perl -e '$(HELP_FUN)' $(MAKEFILE_LIST)



venv: venv-cleanup venv-install##@Env Init venv and install poetry dependencies

venv-cleanup: ##@Env Cleanup venv
	@rm -rf .venv || true
	python -m venv ${VENV}
	${PIP} install -U setuptools wheel pip
	${PIP} install poetry poetry-bumpversion

venv-install: ##@Env Install requirements to venv
	${POETRY} config virtualenvs.create false
	${POETRY} self add poetry-bumpversion
	${POETRY} install --no-root --extras server --extras consumer --with dev,test,docs $(ARGS)
	${PIP} install --no-deps sphinx-plantuml


db: db-start db-upgrade db-partitions ##@DB Prepare database (in docker)

db-start: ##@DB Start database
	docker compose up -d --wait db $(DOCKER_COMPOSE_ARGS)

db-revision: ##@DB Generate migration file
	${POETRY} run python -m data_rentgen.db.migrations revision --autogenerate $(ARGS)

db-upgrade: ##@DB Run migrations to head
	${POETRY} run python -m data_rentgen.db.migrations upgrade head $(ARGS)

db-downgrade: ##@DB Downgrade head migration
	${POETRY} run python -m data_rentgen.db.migrations downgrade head-1 $(ARGS)

db-partitions: ##@DB Create partitions
	${POETRY} run python -m data_rentgen.db.scripts.create_partitions --start 2024-07-01

db-views: ##@DB Create views
	${POETRY} run python -m data_rentgen.db.scripts.refresh_analytic_views $(ARGS)

broker: broker-start ##@Broker Prepare broker (in docker)

broker-start: ##Broker Start broker
	docker compose -f docker-compose.test.yml --profile consumer up -d --wait $(DOCKER_COMPOSE_ARGS)


test: test-db test-broker ##@Test Run tests
	${POETRY} run pytest $(PYTEST_ARGS)

test-lineage: test-db ##@Test Run linege tests
	${POETRY} run pytest -m lineage $(PYTEST_ARGS)

test-server: test-db ##@Test Run server tests
	${POETRY} run pytest -m server $(PYTEST_ARGS)

test-db: test-db-start db-upgrade db-partitions ##@TestDB Prepare database (in docker)

test-db-start: ##@TestDB Start database
	docker compose -f docker-compose.test.yml up -d --wait db $(DOCKER_COMPOSE_ARGS)

test-broker: test-broker-start ##@TestBroker Prepare broker (in docker)

test-broker-start: ##@TestBroker Start broker
	docker compose -f docker-compose.test.yml --profile consumer up -d --wait $(DOCKER_COMPOSE_ARGS)

test-ci: test-db test-broker ##@Test Run CI tests
	${POETRY} run coverage run -m pytest

test-check-fixtures: ##@Test Check declared fixtures
	${POETRY} run pytest --dead-fixtures $(PYTEST_ARGS)

test-cleanup: ##@Test Cleanup tests dependencies
	docker compose -f docker-compose.test.yml --profile all down --remove-orphans $(ARGS)



dev-server: db-start ##@Application Run development server (without docker)
	${POETRY} run python -m data_rentgen.server $(ARGS)

dev-consumer: db-start broker-start ##@Application Run development broker (without docker)
	${POETRY} run python -m data_rentgen.consumer $(ARGS)

prod-build: ##@Application Build docker image
	docker build --progress=plain --network=host -t mtsrus/data-rentgen:develop -f ./docker/Dockerfile $(ARGS) .

prod: ##@Application Run production containers
	docker compose up -d

prod-cleanup: ##@Application Stop production containers
	docker compose down --remove-orphans $(ARGS)


.PHONY: docs

docs: docs-build docs-open ##@Docs Generate & open docs

docs-build: ##@Docs Generate docs
	$(MAKE) -C docs html

docs-open: ##@Docs Open docs
	xdg-open docs/_build/html/index.html

docs-cleanup: ##@Docs Cleanup docs
	$(MAKE) -C docs clean

docs-fresh: docs-cleanup docs-build ##@Docs Cleanup & build docs

openapi: ##@Docs Generate OpenAPI schema
	python -m data_rentgen.server.scripts.export_openapi_schema docs/_static/openapi.json
