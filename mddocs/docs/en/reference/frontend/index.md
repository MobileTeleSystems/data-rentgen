# Frontend { #frontend }

Data.Rentgen provides a [Frontend (UI)](https://github.com/MobileTeleSystems/data-rentgen-ui) based on [ReactAdmin](https://marmelab.com/react-admin/) and [ReactFlow](https://reactflow.dev/),
providing users the ability to navigate entities and build lineage graph.

## Install & run

### With Docker

- Install [Docker](https://docs.docker.com/engine/install/)

- Install [docker-compose](https://github.com/docker/compose/releases/)

- Run the following command:

  ```console
  $ docker compose --profile frontend up -d --wait
  ...
  ```

  `docker-compose` will download Data.Rentgen UI image, create containers, and then start them.

  Options can be set via `.env` file or `environment` section in `docker-compose.yml`

=== "docker-compose.yml"

  ```yaml
    .. literalinclude:: ../../../docker-compose.yml
        :emphasize-lines: 140-151
  ```

=== ".env.docker"

  ```yaml
    .. literalinclude:: ../../../.env.docker
        :emphasize-lines: 36-37
  ```

- After frontend is started and ready, open <http://localhost:3000>.

## See also

[Configuration][configuration-frontend]
