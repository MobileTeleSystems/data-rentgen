(server)=

# REST API Server

Data.Rentgen REST API server provides simple HTTP API for accessing entities stored in {ref}`database`.
Implemented using [FastAPI](https://fastapi.tiangolo.com/).

## Install & run

### With docker

- Install [Docker](https://docs.docker.com/engine/install/)

- Install [docker-compose](https://github.com/docker/compose/releases/)

- Run the following command:

  ```console
  $ docker compose --profile server up -d --wait
  ```

  `docker-compose` will download all necessary images, create containers, and then start the server.

  Options can be set via `.env` file or `environment` section in `docker-compose.yml`

  ```{eval-rst}
  .. dropdown:: ``docker-compose.yml``

    .. literalinclude:: ../../../docker-compose.yml
        :emphasize-lines: 71-99
  ```

  ```{eval-rst}
  .. dropdown:: ``.env.docker``

    .. literalinclude:: ../../../.env.docker
        :emphasize-lines: 22-27
  ```

- After server is started and ready, open <http://localhost:8000/docs>.

### Without docker

- Install Python 3.10 or above

- Setup {ref}`database`, run migrations and create partitions

- Create virtual environment

  ```console
  $ python -m venv /some/.venv
  $ source /some/.venv/activate
  ```

- Install `data-rentgen` package with following *extra* dependencies:

  ```console
  $ pip install data-rentgen[server,postgres]
  ```

- Run server process

  ```console
  $ python -m data_rentgen.server --host 0.0.0.0 --port 8000
  ```

  This is a thin wrapper around [uvicorn](https://www.uvicorn.org/#command-line-options) cli,
  options and commands are just the same.

- After server is started and ready, open <http://localhost:8000/docs>.

## See also

```{toctree}
:maxdepth: 1

auth/index
configuration/index
openapi
```
